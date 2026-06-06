CREATE TRIGGER my_schema.my_table_trg
FOR INSERT OR UPDATE OR DELETE ON my_schema.my_table
COMPOUND TRIGGER
  v_ts        TIMESTAMP(6) := SYSTIMESTAMP;
  v_cnt        NUMBER      := 0;
BEFORE EACH ROW IS
  col_d_value_override INT := 1;
BEGIN

  IF updating THEN
    SELECT COUNT(*)
      INTO v_cnt 
      FROM (SELECT :new.id FROM dual UNION
            SELECT :old.id FROM dual
           );

    IF v_cnt > 1 THEN
      raise_application_error(-20000, 'You can''t change primary key.');
    END IF;

    SELECT COUNT(*)
      INTO v_cnt 
      FROM (SELECT :new.id, :new.col_a, :new.col_b, :new.col_c, :new.col_d FROM dual UNION
            SELECT :old.id, :old.col_a, :old.col_b, :old.col_c, :old.col_d FROM dual
           );
  END IF;

  IF v_cnt = 2 OR inserting THEN :new.col_ts :=    v_ts;
  ELSIF updating            THEN :new.col_ts := :old.col_ts;
  END IF;

  IF v_cnt = 2 OR deleting  THEN
    INSERT INTO my_schema.h$_hub_lnk_source_l
      (id, col_a, col_b, col_c, col_d )
    VALUES
      (:old.id, :old.col_a, :old.col_b, :old.col_c, :col_d_value_override )
    ;
  END IF;

END BEFORE EACH ROW;
END;
