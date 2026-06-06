CREATE CONTROLFILE DATABASE "demo"
LOGFILE
  GROUP 1 '/path/oracle/dbs/t_log1.f'  SIZE 500K,
  GROUP 2 '/path/oracle/dbs/t_log2.f'  SIZE 500K
RESETLOGS
datafile
    '/path/file'
FORCE LOGGING ARCHIVELOG
CHARACTER SET WE8DEC
