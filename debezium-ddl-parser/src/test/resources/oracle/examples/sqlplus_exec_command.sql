-- Short form of the SQL*Plus EXECUTE command.
EXEC dbms_output.put_line('short');

-- Full form.
EXECUTE dbms_output.put_line('full');

-- Documented EXECUTE example: assign a value to a bind variable.
EXECUTE :n := 1

-- Stored procedure call without an argument list.
EXEC hr.my_procedure
