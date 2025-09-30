select
	to_dsinterval('10-02'),
	to_dsinterval('1x-02' default '00-00' on conversion error),
	to_dsinterval(raw_value),
	to_dsinterval(raw_value default '00-00' on conversion error),
	to_dsinterval("RAW_VALUE" default '00-00' on conversion error)
from raw_values;