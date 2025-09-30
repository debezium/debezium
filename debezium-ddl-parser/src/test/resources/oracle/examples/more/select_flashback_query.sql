 select *
    from sale_op_tbl versions period for dt between CURRENT_DATE - 10 and CURRENT_DATE
    where id_store = 589
      and id_item = 29584;
