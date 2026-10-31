"""Transport mode of a GTFS route_type (basic and extended types), as SQL over a `route_type` column."""

MODE_SQL = """
CASE
    WHEN route_type = 0 OR route_type BETWEEN 900 AND 906 THEN 'tram'
    WHEN route_type = 3 OR route_type BETWEEN 700 AND 716 OR route_type BETWEEN 200 AND 209 THEN 'bus'
    WHEN route_type = 11 OR route_type = 800 THEN 'trolleybus'
    WHEN route_type = 1 OR route_type BETWEEN 400 AND 405 THEN 'metro'
    WHEN route_type = 2 OR route_type BETWEEN 100 AND 117 THEN 'rail'
    WHEN route_type = 4 OR route_type = 1000 OR route_type = 1200 THEN 'ferry'
    WHEN route_type = 5 THEN 'cable_tram'
    WHEN route_type = 6 OR route_type BETWEEN 1300 AND 1307 THEN 'aerial_lift'
    WHEN route_type = 7 OR route_type = 1400 THEN 'funicular'
    WHEN route_type = 12 THEN 'monorail'
    ELSE 'other'
END
"""
