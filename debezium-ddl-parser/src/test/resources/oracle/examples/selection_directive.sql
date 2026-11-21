declare
    l_res integer := $if true $then 10 $else 20 $end;

    cursor c is
        select $if true $then 1 $else 2 $end as col from dual;

begin
    $if true $then
        dbms_output.put_line(1);
    $else
    $end

    $if false $then
    $else
        dbms_output.put_line(2);
    $end

    $if false $then
    $elsif true $then
        dbms_output.put_line(3);
    $else
    $end

    $if false $then
        dbms_output.put_line('if');
    $elsif true $then
        dbms_output.put_line('else if');
    $else
        dbms_output.put_line('else');
    $end

    $if true $then
    $end

    $if true $then
    $else
    $end

    $if false $then
    $elsif true $then
    $end

    $if false $then
    $elsif true $then
    $else
    $end

    select 1
    into l_res
    from dual
    order by
     $if true $then
      1
     $else
      2
     $end
     ;

    l_res := to_number($if true $then '1' $else '2' $end);

    if $if true $then 1 $else 2 $end = 1 then
        null;
    end if;

end;
