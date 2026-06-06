-- From https://www.oratable.com/querying-json-data-in-oracle/
-- 2.2a  SQL/JSON query: JSON_QUERY
--       to select the entire JSON document
select custid
     , custname
     , json_query(metadata, '$'
                  ) json_metadata
from customer;