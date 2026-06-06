select
	raw_value,
	validate_conversion(raw_value as number),
	validate_conversion(raw_value as date, 'YYYY-MM-DD'),
	validate_conversion(raw_value as date, 'YYYY-MM-DD', 'NLS_DATE_LANGUAGE = RUSSIAN')
from raw_values;