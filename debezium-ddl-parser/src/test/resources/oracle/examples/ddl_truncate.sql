truncate table APP_PART_NOTES_4 drop storage;
TRUNCATE TABLE WS.LOGS PRESERVE MATERIALIZED VIEW LOG DROP STORAGE;
TRUNCATE CLUSTER personnel REUSE STORAGE;
