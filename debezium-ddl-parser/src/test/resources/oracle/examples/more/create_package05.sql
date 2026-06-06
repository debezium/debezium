create or replace package pkg_fun_with_streaming is

  type t_change is record(
     guid   raw(16)
   );


  type t_message is record(
    guid raw(16)
  );

  type t_change_cur is ref cursor return t_change;

  type t_message_tab is table of t_message not null index by int;


  function prep_messages(p_changes t_change_cur)
    return t_message_tab
    pipelined cluster p_changes by(guid)
    parallel_enable(partition p_changes by hash(guid));

end;
/

create or replace package body pkg_fun_with_streaming is

  function prep_messages(p_changes t_change_cur)
    return t_message_tab
    pipelined cluster p_changes by(guid)
    parallel_enable(partition p_changes by hash(guid)) is

    l_rec     t_change;
    l_message t_message;

  begin

    loop
      fetch p_changes
        into l_rec;


        if l_rec.guid is not null
        then
          pipe row(l_message);

        end if;

    end loop;

  end prep_messages;


end pkg_fun_with_streaming;
/