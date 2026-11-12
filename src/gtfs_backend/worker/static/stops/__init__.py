"""Match a new feed version's stops to our stable stops / stop groups.

A feed's stop is first put into GTFS shape (model.normalize_stop): its station is its parent_station, or what the
feed's stop_code_pattern reads from its code (ZTP: `802-01` is platform_code 01 of station 802). Stops (physical
poles) are identified across a namespace's feeds by `stop_code`; stop groups (logical stops) by that station. Our
IDs never derive from codes or names, so renames and renumberings keep the ID.

plan.py (own namespace) and shared.py (another namespace's poles, e.g. MLD on ZTP's) decide, without the
database, and are unit-tested; apply.py reads the inputs, writes the result and refreshes state.py.
"""

from gtfs_backend.worker.static.stops.apply import apply_matches
from gtfs_backend.worker.static.stops.model import Group, NewStop, Stop, normalize_stop
from gtfs_backend.worker.static.stops.names import names_match
from gtfs_backend.worker.static.stops.plan import plan_matches
from gtfs_backend.worker.static.stops.shared import plan_shared

__all__ = [
    "Stop",
    "Group",
    "NewStop",
    "apply_matches",
    "names_match",
    "normalize_stop",
    "plan_matches",
    "plan_shared",
]
