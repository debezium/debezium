-- Initial pause to ensure that SQL Server job start is completed (CI timing)
WAITFOR DELAY '00:00:15';

-- Create the test database
CREATE DATABASE testDB;

USE testDB;
EXEC sys.sp_cdc_enable_db;

ALTER DATABASE testDB SET ALLOW_SNAPSHOT_ISOLATION ON;

CREATE TABLE ignore (id integer);
INSERT INTO ignore values (1);

EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'ignore', @role_name = NULL, @supports_net_changes = 0;



