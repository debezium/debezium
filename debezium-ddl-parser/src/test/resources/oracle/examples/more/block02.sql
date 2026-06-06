-- anonymous block with undocumented NULL option in variable declaration
DECLARE
  i INTEGER NULL DEFAULT 0;
BEGIN
  dbms_output.put_line(i);
END;