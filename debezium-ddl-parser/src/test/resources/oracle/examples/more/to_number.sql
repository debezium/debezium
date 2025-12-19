select
	raw_value,
	to_number(raw_value),
	to_number(raw_value, '9990.9999'),
	to_number(raw_value, '9990.9999', 'NLS_DATE_LANGUAGE = RUSSIAN'),
	to_number(raw_value default 0 on conversion error),
	to_number(raw_value default 0 on conversion error, '9990.9999'),
	to_number(raw_value default 0 on conversion error, '9990.9999', 'NLS_DATE_LANGUAGE = RUSSIAN')
from raw_values;