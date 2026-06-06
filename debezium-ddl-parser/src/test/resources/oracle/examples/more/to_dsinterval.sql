select
	to_dsinterval('10 1:02:10'),
	to_dsinterval('1o 1:02:10' default '10 8:00:00' on conversion error),
	to_dsinterval(raw_value),
	to_dsinterval(raw_value default '10 8:00:00' on conversion error),
	to_dsinterval("RAW_VALUE" default '10 8:00:00' on conversion error)
from raw_values;