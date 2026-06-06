create or replace trigger trg_status_tick
 after insert or update or delete on status for each row
declare
begin
 if inserting then
   tp_ticket_util.fire('status', tp_ticket_const.cinsert, :new.id);
 elsif updating then
   tp_ticket_util.fire('status', tp_ticket_const.cupdate, :new.id);
 elsif deleting then
   tp_ticket_util.fire('status', tp_ticket_const.cdelete, :old.id);
 end if;
end;