CREATE OR REPLACE PROCEDURE prc_calendar_data (cal_cv IN OUT calendar_data.calcurtyp, year_id INT)
    PARALLEL_ENABLE
    /* although PARALLEL_ENABLE is legal for procedures,
       the following part of the clause will cause PLS-00655 error, if uncommented:
       (PARTITION cal_cv BY ANY) */
    AS
BEGIN
   OPEN cal_cv FOR SELECT *
      FROM d_months
      WHERE year_id = year_id;
EXCEPTION WHEN OTHERS THEN
  raise_application_error(-20001, 'Error!');
END;
