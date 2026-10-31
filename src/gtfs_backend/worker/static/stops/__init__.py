"""Match a new feed version's stops to our stable stop platforms / stop groups.

Platforms (physical stops) are identified across feeds by ZTP's `stop_code` (`GGG-PP`); groups (logical
stops) by the code prefix `GGG`. Our IDs never derive from codes or names, so renames and renumberings
keep the ID.

plan.py (own namespace) and shared.py (another namespace's poles, e.g. MLD on ZTP's) decide, without the
database, and are unit-tested; apply.py reads the inputs, writes the result and refreshes state.py.
"""

from gtfs_backend.worker.static.stops.apply import apply_matches
from gtfs_backend.worker.static.stops.model import Group, NewStop, Platform
from gtfs_backend.worker.static.stops.names import names_match
from gtfs_backend.worker.static.stops.plan import plan_matches
from gtfs_backend.worker.static.stops.shared import plan_shared

__all__ = ["Group", "NewStop", "Platform", "apply_matches", "names_match", "plan_matches", "plan_shared"]
