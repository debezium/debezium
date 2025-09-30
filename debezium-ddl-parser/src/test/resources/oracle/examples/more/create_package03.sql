CREATE OR REPLACE PACKAGE pkg_prc_with_properties AS
  PROCEDURE access_prc_with_properties;

  PRAGMA DEPRECATE(prc_with_properties, 'no more supported');

  PROCEDURE prc_with_properties
    PARALLEL_ENABLE
    ACCESSIBLE BY (PROCEDURE access_prc_with_properties);

  END pkg_prc_with_properties;
/

CREATE OR REPLACE PACKAGE BODY pkg_prc_with_properties AS
  PROCEDURE access_prc_with_properties
    IS
  BEGIN
     prc_with_properties();
  END access_prc_with_properties;

  PROCEDURE prc_with_properties
    PARALLEL_ENABLE
    ACCESSIBLE BY (PROCEDURE access_prc_with_properties)
    IS
  BEGIN
     DBMS_OUTPUT.PUT_LINE('I have properties!');
  END prc_with_properties;

END pkg_prc_with_properties;
/

CALL pkg_prc_with_properties.access_prc_with_properties();
