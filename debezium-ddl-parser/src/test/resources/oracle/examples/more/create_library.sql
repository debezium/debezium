CREATE LIBRARY ext_lib AS 'ddl_1' IN ddl_dir;
CREATE OR REPLACE LIBRARY ext_lib AS 'ddl_1' IN ddl_dir CREDENTIAL ddl_cred;
CREATE LIBRARY ext_lib AS '/OR/lib/ext_lib.so';
CREATE OR REPLACE LIBRARY ext_lib IS '/OR/newlib/ext_lib.so';
CREATE LIBRARY app_lib as '${ORACLE_HOME}/lib/app_lib.so' AGENT 'sales.hq.example.com';