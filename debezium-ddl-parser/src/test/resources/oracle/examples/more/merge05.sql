-- MERGE INTO with a rowtype variable as "insert values" parameter
MERGE INTO salesfront.SF_FORM_REFERENCE_CHOICES_DEF target
	USING (select null from dual)
	ON (recChoicesDef.id = target.id)
WHEN MATCHED THEN
	UPDATE SET target.source_id = recChoicesDef.source_id
WHEN NOT MATCHED THEN
	INSERT VALUES recChoicesDef;