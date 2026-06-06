CREATE SPFILE
   FROM PFILE = '$ORACLE_HOME/work/t_init1.ora';

CREATE SPFILE = 's_params.ora'
   FROM PFILE = '$ORACLE_HOME/work/t_init1.ora';

CREATE SPFILE = 's_params.ora'
   FROM PFILE = '$ORACLE_HOME/work/t_init1.ora' AS COPY;

CREATE SPFILE = 's_params.ora'
   FROM MEMORY;
