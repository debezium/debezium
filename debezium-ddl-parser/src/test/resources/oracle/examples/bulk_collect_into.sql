declare
    type values_t is table of varchar2(1);
    l_values values_t;
begin
    select dummy bulk collect into l_values from dual;
end;
/

select dummy bulk from dual;
