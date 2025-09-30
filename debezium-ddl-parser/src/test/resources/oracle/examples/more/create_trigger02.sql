create or replace trigger my_trg
  before insert on my_schema.tgt_table references
  for each row
declare
  l_id          integer;
  l_rqsttype_id integer;
  l_rest_date   date;
  v_cnt         number(1);
begin
  if :new.id is null
  then
    :new.id := gen_id('seq', 1);
  else
    l_id := gen_id('seq', 0);
    if :new.id > l_id
    then
      :new.id := gen_id('seq', :new.id - l_id);
    end if;
  end if;
  :new.date_load := sysdate;
  :new.mi_part   := floor(to_char(sysdate, 'mi') / 10);
  begin
    select count(1)
      into v_cnt
      from my_schema.nem_tbl rt
     where rt.rqsttypeid = l_rqsttype_id;

    if (v_cnt = 0)
    then
      l_rest_date := add_months(trunc(l_rest_date, 'mm'), 1) - 1;
    end if;

    :new.date_rest := l_rest_date;

  exception
    when others then
      :new.date_rest := null;
  end;
end;
