select
	raw_value,
	to_binary_float(raw_value),
	to_binary_float(raw_value, '9990.9999'),
	to_binary_float(raw_value, '9990.9999', 'NLS_DATE_LANGUAGE = RUSSIAN'),
	to_binary_float(raw_value default 0 on conversion error),
	to_binary_float(raw_value default 0 on conversion error, '9990.9999'),
	to_binary_float(raw_value default 0 on conversion error, '9990.9999', 'NLS_DATE_LANGUAGE = RUSSIAN')
from raw_values;