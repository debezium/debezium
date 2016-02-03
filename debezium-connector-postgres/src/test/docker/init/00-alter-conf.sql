-- Change the system configuration

ALTER SYSTEM SET max_replication_slots = 10;
ALTER SYSTEM SET wal_level = 'logical';