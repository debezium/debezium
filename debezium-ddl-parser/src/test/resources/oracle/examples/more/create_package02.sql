CREATE OR REPLACE PACKAGE calendar_data AUTHID DEFINER AS
       TYPE calcurtyp IS REF CURSOR RETURN d_months%ROWTYPE;
       PROCEDURE open_cal_cv (cal_cv IN OUT calcurtyp, year_id INT);
END calendar_data;

CREATE OR REPLACE PACKAGE BODY calendar_data AS
  PROCEDURE open_cal_cv (cal_cv IN OUT calcurtyp, year_id INT)
  PARALLEL_ENABLE
  /* although PARALLEL_ENABLE is legal for procedures,
       the following part of the clause will cause PLS-00655 error, if uncommented:
       (PARTITION cal_cv BY ANY) */
  IS
  BEGIN
      OPEN cal_cv FOR SELECT *
      FROM d_months
      WHERE year_id = year_id;
  END open_cal_cv;
END calendar_data;
