CREATE OR REPLACE PACKAGE initialized_package AS

  END initialized_package;
/

CREATE OR REPLACE PACKAGE BODY initialized_package AS


  BEGIN
    DBMS_OUTPUT.PUT_LINE('Initialized');
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE('ERROR!');
      RAISE;
END initialized_package;
/