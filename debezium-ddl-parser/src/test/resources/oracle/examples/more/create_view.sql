CREATE OR REPLACE VIEW VIEW_TEST_ROLE AS
SELECT ID, NAME || '-' ALIAS_NAME
FROM TEST_ROLE
WHERE ID < 6 with check option;

CREATE OR REPLACE FORCE VIEW VIEW_TEST_ROLE AS
SELECT ID, NAME || '-' ALIAS_NAME
FROM TEST_ROLE
WHERE ID < 7 with check option;

CREATE OR REPLACE NO FORCE VIEW VIEW_TEST_ROLE AS
SELECT ID, NAME || '-' ALIAS_NAME
FROM TEST_ROLE
WHERE ID < 8 with check option;

CREATE VIEW warehouse_view OF XMLTYPE
    XMLSCHEMA "http://www.example.com/xwarehouses.xsd"
    ELEMENT "Warehouse"
    WITH OBJECT ID
    (extract(OBJECT_VALUE, '/Warehouse/Area/text()').getnumberval())
AS SELECT XMLELEMENT("Warehouse",
                     XMLFOREST(WarehouseID as "Building",
                               area as "Area",
                               docks as "Docks",
                               docktype as "DockType",
                               wateraccess as "WaterAccess",
                               railaccess as "RailAccess",
                               parking as "Parking",
                               VClearance as "VClearance"))
   FROM warehouse_table;

CREATE VIEW THE_VIEW IF NOT EXISTS AS
SELECT ID, NAME
FROM THE_TABLE
WHERE ID > 10;
