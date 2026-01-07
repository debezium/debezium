create type ty_a is object (b VARCHAR2(10));
/
CREATE TYPE TYT_a IS TABLE OF TY_A;
/
CREATE TYPE TYT_VARCHAR2 IS TABLE OF VARCHAR2(10);
/
declare
l_ty_a ty_a;
L_TYT_A TYT_a:=TYT_a();
L_TYT_VARCHAR2 TYT_VARCHAR2:=TYT_VARCHAR2();
L INT;
begin
  l_ty_a:=ty_a(1);
  L_TYT_A.EXTEND;
  L_TYT_A(1):=l_ty_a;
  L_TYT_VARCHAR2:=TYT_VARCHAR2('1');
if L_TYT_A(1).B not member of L_TYT_VARCHAR2 then 
  dbms_output.put_line('Y');
  else 
  dbms_output.put_line('N');
end if;
L:=CASE WHEN L_TYT_A(1).B not member of L_TYT_VARCHAR2 THEN 1 END;
end;
/