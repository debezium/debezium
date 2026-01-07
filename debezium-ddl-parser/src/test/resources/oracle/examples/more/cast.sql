select
	raw_value,
	cast(raw_value as date, 'YYYY-MM-DD'),
	cast(raw_value as date, 'YYYY-MM-DD', 'NLS_DATE_LANGUAGE = RUSSIAN'),
	cast(raw_value as number default null on conversion error),
	cast(raw_value as number default 9999 on conversion error),
	cast(raw_value as date default null on conversion error, 'YYYY-MM-DD'),
	cast(raw_value as date default null on conversion error, 'YYYY-MM-DD', 'NLS_DATE_LANGUAGE = RUSSIAN')
from raw_values;