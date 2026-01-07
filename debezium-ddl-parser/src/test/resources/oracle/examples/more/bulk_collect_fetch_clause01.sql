declare
    cursor cur_div is
        select rid, code from tmp_data_item_value_euro;
    lvnCount number := 0;
    type cCurDiv_t is table of cur_div%Rowtype;
    lvaCurDiv cCurDiv_t;
begin
    open cur_div;
    
    loop
        fetch cur_div bulk collect into lvaCurDiv limit 100000;
        exit when lvaCurDiv.count = 0;
 
        forall i in lvaCurDiv.first .. lvaCurDiv.last
            update data_item_value set unit_code = lvaCurDiv(i).code where rowid = lvaCurDiv(i).rid;      
            
        commit;
    end loop;
	close cur_div;
    commit;
end;