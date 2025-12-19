CREATE OR REPLACE EDITIONABLE FUNCTION "TEST"."TRIM_FUNC" (vField in varchar2) return varchar2 is
    vTemp varchar2(100);
begin
    vTemp:=rtrim(ltrim(vField));
    return vTemp;
end;