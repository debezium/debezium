SELECT *
FROM json_documents
WHERE json_document IS JSON (STRICT);

SELECT *
FROM json_documents
WHERE json_document IS NOT JSON (LAX);
