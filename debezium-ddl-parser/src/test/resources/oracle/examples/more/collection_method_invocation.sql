DECLARE
   TYPE nums_list IS VARRAY(3) OF INTEGER;

   ns nums_list := nums_list(1, 2, 3);
BEGIN

   DBMS_OUTPUT.PUT_LINE(ns.first);
   DBMS_OUTPUT.PUT_LINE(ns.prior(3));
   DBMS_OUTPUT.PUT_LINE(ns.last);


   ns.delete;
   DBMS_OUTPUT.PUT_LINE(ns.count);
END;
/