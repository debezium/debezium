create or replace package pkgtest is

    cursor cuData is
        select * from dual;

    procedure main;
end;
/

create or replace package body pkgtest is
    procedure main is
        sbData varchar2(100);
    begin
      open pkgtest.cuData;
      fetch cuData into sbData;
      close cuData;

      if cuData%isopen then
        dbms_output.put_line('should work');
      end if;

      if sql%rowcount > 0 then
        dbms_output.put_line('should work too');
      end if;
    end;

    function GETPOS_TEMPLATE_DEF_ID return VARCHAR2 is
      LVS_ID VARCHAR2(4000 CHAR);
    BEGIN
      LVS_ID := TRIM(BOTH SF_FORM_EXT_CONSTANTS.GETSOMETHING FROM LVS_ID);
      return LVS_ID;
    end GETPOS_TEMPLATE_DEF_ID;
end;
/
