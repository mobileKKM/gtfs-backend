"""Stop poles from an OSM extract (stops/osm.py read_extract)."""

from gtfs_backend.worker.static.stops.osm import read_extract

EXTRACT = """<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6">
  <node id="1" lat="50.0" lon="19.0"><tag k="highway" v="bus_stop"/><tag k="ref" v="802-2"/></node>
  <node id="2" lat="50.1" lon="19.1"/>
  <node id="3" lat="50.2" lon="19.2"/>
  <node id="4" lat="50.3" lon="19.3"/>
  <node id="5" lat="50.4" lon="19.4"><tag k="public_transport" v="platform"/></node>
  <way id="10">
    <nd ref="2"/><nd ref="3"/><nd ref="4"/>
    <tag k="public_transport" v="platform"/><tag k="tram" v="yes"/><tag k="name" v="Teatr"/>
  </way>
  <way id="11">
    <nd ref="2"/><nd ref="99"/><nd ref="4"/>
    <tag k="railway" v="tram_stop"/><tag k="public_transport" v="stop_position"/>
  </way>
  <way id="12"><nd ref="2"/><nd ref="3"/><tag k="highway" v="residential"/></way>
</osm>
"""


def test_poles_are_tagged_nodes_and_platform_ways_at_their_middle_node(tmp_path):
    path = tmp_path / "extract.osm"
    path.write_text(EXTRACT)
    poles = {p.osm_id: p for p in read_extract(path)}
    # node 5 says it's a platform but not what stops there; way 12 is a street
    assert sorted(poles) == ["node/1", "way/10", "way/11"]
    assert (poles["node/1"].lat, poles["node/1"].code, poles["node/1"].bus) == (50.0, "802-02", True)
    assert (poles["way/10"].lat, poles["way/10"].lon, poles["way/10"].tram, poles["way/10"].name) == (50.2, 19.2, True, "Teatr")
    # node 99 isn't in the extract: the middle of the nodes that are (2, 4)
    assert (poles["way/11"].lat, poles["way/11"].kind) == (50.3, "stop_position")
