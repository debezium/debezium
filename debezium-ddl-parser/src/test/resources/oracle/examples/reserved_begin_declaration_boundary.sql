create or replace package body reserved_begin_declaration_boundary as
    function run(p_value in number) return boolean
    is
    begin
        begin
            null;
        exception
            when others then
                return false;
        end;
        p_value := p_value + 1;
        return true;
    end run;
end reserved_begin_declaration_boundary;
/
