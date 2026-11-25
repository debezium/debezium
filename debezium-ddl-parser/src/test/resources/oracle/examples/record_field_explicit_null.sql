CREATE PACKAGE record_field_explicit_null AS
    TYPE item_record IS RECORD (
        value DATE NULL,
        description VARCHAR2(100) NOT NULL
    );
END record_field_explicit_null;
/
