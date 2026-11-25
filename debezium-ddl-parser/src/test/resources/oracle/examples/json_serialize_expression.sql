SELECT JSON_SERIALIZE(json_document RETURNING CLOB)
FROM json_documents;

SELECT JSON_SERIALIZE(
    CASE WHEN use_fallback = 1 THEN '{}' ELSE json_document END
    RETURNING CLOB PRETTY
)
FROM json_documents;
