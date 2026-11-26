// Load test: CLIENTS app clients polling the REST API the way the app does (docs/benchmark.md).
//
//   k6 run bench/k6/rest_mix.js                                   # 2000 clients, 1 min ramp, 10 min steady
//   k6 run -e CLIENTS=200 -e DURATION=2m bench/k6/rest_mix.js     # a quick one
//
// Each client opens a view and polls it every POLL_S seconds (with If-None-Match, as the app does), then opens the next:
//   map 50 %         /stops?bbox once, then /vehicles?bbox (a 2-8 km box somewhere in Krakow)
//   board 30 %       /stops/{key}/departures
//   trip 15 %        /trips/{key} once, then /trips/{key}/live
//   vehicle 5 %      /vehicles/{key}/trip
// and searches (/stops/search) before 10 % of the views. Results per request name: p50/p90/p99 and 304 share, in the
// printed summary and in SUMMARY (JSON).

import http from 'k6/http';
import { sleep } from 'k6';
import { Counter } from 'k6/metrics';

const BASE = __ENV.BASE_URL || 'http://127.0.0.1:8000/api/v1';
const CLIENTS = parseInt(__ENV.CLIENTS || '2000', 10);
const RAMP = __ENV.RAMP || '1m';
const DURATION = __ENV.DURATION || '10m';
const POLL_S = parseFloat(__ENV.POLL_S || '10');
const SUMMARY = __ENV.SUMMARY || 'bench/out/k6-summary.json';
const CITY = [19.8, 49.97, 20.15, 50.13]; // minLon, minLat, maxLon, maxLat
const NAMES = ['vehicles', 'stops_map', 'board', 'trip', 'trip_live', 'vehicle_trip', 'search'];
const QUERIES = ['rondo', 'dworzec', 'plac', 'nowy', 'bronowice', 'kurdwanow', 'czerwone maki', 'teatr', 'wawel', 'krowodrza'];

const notModified = new Counter('not_modified');

export const options = {
  scenarios: {
    clients: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: RAMP, target: CLIENTS },
        { duration: DURATION, target: CLIENTS },
      ],
      gracefulRampDown: '10s',
    },
  },
  summaryTrendStats: ['avg', 'p(50)', 'p(90)', 'p(99)', 'max', 'count'],
  // a trip or vehicle that has left the feed answers 404, as it does for the app
  thresholds: Object.assign(
    { http_req_failed: ['rate<0.01'] },
    ...NAMES.map((n) => ({ [`http_req_duration{name:${n}}`]: ['p(99)<60000'] })), // only so each name is reported
    ...NAMES.map((n) => ({ [`not_modified{name:${n}}`]: ['count>=0'] })),
  ),
};

http.setResponseCallback(http.expectedStatuses(200, 304, 404));

export function setup() {
  const stops = http.get(`${BASE}/stops?bbox=${CITY.join(',')}&kind=group`).json('stops') || [];
  const vehicles = http.get(`${BASE}/vehicles?bbox=${CITY.join(',')}`).json('vehicles') || [];
  if (!stops.length || !vehicles.length) {
    throw new Error(`no data to test with (${stops.length} stops, ${vehicles.length} vehicles): is the worker running?`);
  }
  return { stops: stops.map((s) => s.key), trips: vehicles.map((v) => v.trip_key), vehicles: vehicles.map((v) => v.key) };
}

const etags = {}; // per client (VU): url -> ETag of the body it has

function get(url, name) {
  const headers = etags[url] ? { 'If-None-Match': etags[url] } : {};
  const res = http.get(url, { headers, tags: { name } });
  if (res.status === 200 && res.headers.Etag) etags[url] = res.headers.Etag;
  if (res.status === 304) notModified.add(1, { name });
  return res;
}

const pick = (list) => list[Math.floor(Math.random() * list.length)];
const between = (lo, hi) => lo + Math.random() * (hi - lo);
const enc = encodeURIComponent;

function box() {
  const w = between(2, 8) / 71.5; // km -> degrees of longitude at 50 N
  const h = between(2, 8) / 111.0;
  const lon = between(CITY[0], CITY[2] - w);
  const lat = between(CITY[1], CITY[3] - h);
  return [lon, lat, lon + w, lat + h].map((x) => x.toFixed(5)).join(',');
}

function view(data) {
  const r = Math.random();
  if (r < 0.5) {
    const bbox = box();
    get(`${BASE}/stops?bbox=${bbox}`, 'stops_map');
    return () => get(`${BASE}/vehicles?bbox=${bbox}`, 'vehicles');
  }
  if (r < 0.8) {
    const key = pick(data.stops);
    return () => get(`${BASE}/stops/${enc(key)}/departures`, 'board');
  }
  if (r < 0.95) {
    const key = pick(data.trips);
    get(`${BASE}/trips/${enc(key)}`, 'trip');
    return () => get(`${BASE}/trips/${enc(key)}/live`, 'trip_live');
  }
  const key = pick(data.vehicles);
  return () => get(`${BASE}/vehicles/${enc(key)}/trip`, 'vehicle_trip');
}

export default function (data) {
  if (Math.random() < 0.1) get(`${BASE}/stops/search?q=${enc(pick(QUERIES))}`, 'search');
  const poll = view(data);
  const polls = 3 + Math.floor(Math.random() * 10);
  sleep(Math.random() * POLL_S); // clients don't poll in step
  for (let i = 0; i < polls; i++) {
    poll();
    sleep(POLL_S);
  }
}

export function handleSummary(data) {
  const rows = {};
  for (const name of NAMES) {
    const d = data.metrics[`http_req_duration{name:${name}}`];
    if (!d || !d.values.count) continue;
    const nm = data.metrics[`not_modified{name:${name}}`];
    const v = d.values;
    rows[name] = {
      count: v.count,
      p50_ms: v['p(50)'],
      p90_ms: v['p(90)'],
      p99_ms: v['p(99)'],
      max_ms: v.max,
      not_modified_share: nm ? nm.values.count / v.count : 0,
    };
  }
  const failed = data.metrics.http_req_failed ? data.metrics.http_req_failed.values.rate : null;
  const out = { clients: CLIENTS, duration: DURATION, poll_s: POLL_S, failed_rate: failed, requests: rows };
  const fmt = (x) => (x === undefined ? '-' : x.toFixed(1).padStart(8));
  let text = `\n${'request'.padEnd(14)}${'count'.padStart(8)}${'p50'.padStart(8)}${'p90'.padStart(8)}${'p99'.padStart(8)}${'max'.padStart(8)}  304\n`;
  for (const [name, r] of Object.entries(rows)) {
    text += `${name.padEnd(14)}${String(r.count).padStart(8)}${fmt(r.p50_ms)}${fmt(r.p90_ms)}${fmt(r.p99_ms)}${fmt(r.max_ms)}  ${(100 * r.not_modified_share).toFixed(0)} %\n`;
  }
  text += `failed: ${failed === null ? '-' : (100 * failed).toFixed(2) + ' %'} (ms; 404s of trips and vehicles that left count as ok)\n`;
  return { stdout: text, [SUMMARY]: JSON.stringify(out, null, 1) };
}
