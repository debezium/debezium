SELECT (t.t_queue_type).date_calc AS date_calc_par
     , t.t_queue_type.date_calc AS date_calc_direct
     , t.t_queue_type.as_code() AS type_code
     , (t.t_queue_type).as_code() AS type_code_par
     , t.t_queue_type.sub_type.as_code() AS subtype_code
     , ((t.t_queue_type).sub_type).as_code() AS subtype_code_par
     , t.hier(1)(2)(3).gettype AS nested_invocations_and_member_access
     , treat(ent as my_schema.my_table).version AS member_of_treat_result
  FROM t_queue t;
