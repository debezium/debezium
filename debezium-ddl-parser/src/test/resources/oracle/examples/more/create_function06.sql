CREATE OR REPLACE TYPE DUMMY_TAB
AS TABLE OF INTEGER;

-- https://docs.oracle.com/cd/B10501_01/appdev.920/a96624/08_subs.htm#19845
CREATE OR REPLACE FUNCTION fun_pipelined (i INTEGER)
RETURN dummy_tab PIPELINED PARALLEL_ENABLE
AS
BEGIN
       FOR rec1 in (SELECT i AS outrec from dual)
       LOOP
         PIPE ROW (rec1.outrec);
       END LOOP;

 RETURN;
END;
