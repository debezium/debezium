CREATE TABLE tbl (id NUMBER);


DECLARE
   TYPE nums IS TABLE OF NUMBER;
   TYPE nums_list IS VARRAY(2) OF nums;

   ns nums_list := nums_list(nums(1, 2, 3), nums(4, 5, 6));
BEGIN

   FOR i IN 1..2 LOOP
     FORALL j IN INDICES OF ns(i)
        INSERT INTO tbl VALUES (ns(i)(j));
   END LOOP;
END;
/