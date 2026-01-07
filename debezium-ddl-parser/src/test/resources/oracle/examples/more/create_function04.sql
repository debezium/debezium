ALTER SESSION SET PLSQL_CCFLAGS = 'my_debug:TRUE';

CREATE OR REPLACE FUNCTION conditional_compilation(val VARCHAR2)
    RETURN VARCHAR2
  IS

BEGIN
    $IF $$my_debug $THEN
      RETURN '--DEBUG--';
    $ELSE
      RETURN val;
    $END
END;
/

-- SELECT conditional_compilation('hello') FROM DUAL;