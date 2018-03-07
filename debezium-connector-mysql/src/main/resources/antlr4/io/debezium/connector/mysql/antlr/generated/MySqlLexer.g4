/*
MySQL (Positive Technologies) grammar
The MIT License (MIT).
Copyright (c) 2015-2017, Ivan Kochurkin (kvanttt@gmail.com), Positive Technologies.
Copyright (c) 2017, Ivan Khudyashev (IHudyashov@ptsecurity.com)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

lexer grammar MySqlLexer;

channels { MYSQLCOMMENT, ERRORCHANNEL }

// SKIP

SPACE:                               [ \t\r\n]+    -> channel(HIDDEN);
SPEC_MYSQL_COMMENT:                  '/*!' .+? '*/' -> channel(MYSQLCOMMENT);
COMMENT_INPUT:                       '/*' .*? '*/' -> channel(HIDDEN);
LINE_COMMENT:                        (
                                       ('-- ' | '#') ~[\r\n]* ('\r'? '\n' | EOF) 
                                       | '--' ('\r'? '\n' | EOF) 
                                     ) -> channel(HIDDEN);


// Keywords
// Common Keywords


ADD:                                 A D D ;
ALL:                                 A L L ;
ALTER:                               A L T E R ;
ANALYZE:                             A N A L Y Z E ;
AND:                                 A N D ;
AS:                                  A S ;
ASC:                                 A S C ;
BEFORE:                              B E F O R E ;
BETWEEN:                             B E T W E E N ;
BOTH:                                B O T H ;
BY:                                  B Y;
CALL:                                C A L L ;
CASCADE:                             C A S C A D E ;
CASE:                                C A S E ; 
CAST:                                C A S T ; 
CHANGE:                              C H A N G E ; 
CHARACTER:                           C H A R A C T E R ; 
CHECK:                               C H E C K ; 
COLLATE:                             C O L L A T E ; 
COLUMN:                              C O L U M N ; 
CONDITION:                           C O N D I T I O N ; 
CONSTRAINT:                          C O N S T R A I N T ; 
CONTINUE:                            C O N T I N U E ; 
CONVERT:                             C O N V E R T ; 
CREATE:                              C R E A T E ; 
CROSS:                               C R O S S ; 
CURRENT_USER:                        C U R R E N T '_' U S E R ; 
CURSOR:                              C U R S O R ; 
DATABASE:                            D A T A B A S E ; 
DATABASES:                           D A T A B A S E S ; 
DECLARE:                             D E C L A R E ; 
DEFAULT:                             D E F A U L T ; 
DELAYED:                             D E L A Y E D ; 
DELETE:                              D E L E T E ; 
DESC:                                D E S C ; 
DESCRIBE:                            D E S C R I B E ; 
DETERMINISTIC:                       D E T E R M I N I S T I C ; 
DISTINCT:                            D I S T I N C T ; 
DISTINCTROW:                         D I S T I N C T R O W ; 
DROP:                                D R O P ; 
EACH:                                E A C H ; 
ELSE:                                E L S E ; 
ELSEIF:                              E L S E I F ; 
ENCLOSED:                            E N C L O S E D ; 
ESCAPED:                             E S C A P E D ; 
EXISTS:                              E X I S T S ; 
EXIT:                                E X I T ; 
EXPLAIN:                             E X P L A I N ; 
FALSE:                               F A L S E ; 
FETCH:                               F E T C H ; 
FOR:                                 F O R ; 
FORCE:                               F O R C E ; 
FOREIGN:                             F O R E I G N ; 
FROM:                                F R O M ; 
FULLTEXT:                            F U L L T E X T ; 
GRANT:                               G R A N T ; 
GROUP:                               G R O U P ; 
HAVING:                              H A V I N G ; 
HIGH_PRIORITY:                       H I G H '_' P R I O R I T Y ; 
IF:                                  I F ; 
IGNORE:                              I G N O R E ; 
IN:                                  I N ; 
INDEX:                               I N D E X ; 
INFILE:                              I N F I L E ; 
INNER:                               I N N E R ; 
INOUT:                               I N O U T ; 
INSERT:                              I N S E R T ; 
INTERVAL:                            I N T E R V A L ; 
INTO:                                I N T O ; 
IS:                                  I S ; 
ITERATE:                             I T E R A T E ; 
JOIN:                                J O I N ; 
KEY:                                 K E Y ; 
KEYS:                                K E Y S ; 
KILL:                                K I L L ; 
LEADING:                             L E A D I N G ; 
LEAVE:                               L E A V E ; 
LEFT:                                L E F T ; 
LIKE:                                L I K E ; 
LIMIT:                               L I M I T ; 
LINEAR:                              L I N E A R ; 
LINES:                               L I N E S ; 
LOAD:                                L O A D ; 
LOCK:                                L O C K ; 
LOOP:                                L O O P ; 
LOW_PRIORITY:                        L O W '_' P R I O R I T Y ; 
MASTER_BIND:                         M A S T E R '_' B I N D ; 
MASTER_SSL_VERIFY_SERVER_CERT:       M A S T E R '_' S S L '_' V E R I F Y '_' S E R V E R '_' C E R T;
MATCH:                               M A T C H ; 
MAXVALUE:                            M A X V A L U E ; 
MODIFIES:                            M O D I F I E S ; 
NATURAL:                             N A T U R A L ; 
NOT:                                 N O T ; 
NO_WRITE_TO_BINLOG:                  N O '_' W R I T E '_' T O '_' B I N L O G ; 
NULL_LITERAL:                        N U L L ; 
ON:                                  O N ; 
OPTIMIZE:                            O P T I M I Z E ; 
OPTION:                              O P T I O N ; 
OPTIONALLY:                          O P T I O N A L L Y ; 
OR:                                  O R ; 
ORDER:                               O R D E R ; 
OUT:                                 O U T ; 
OUTER:                               O U T E R ; 
OUTFILE:                             O U T F I L E ; 
PARTITION:                           P A R T I T I O N ; 
PRIMARY:                             P R I M A R Y ; 
PROCEDURE:                           P R O C E D U R E ; 
PURGE:                               P U R G E ; 
RANGE:                               R A N G E ; 
READ:                                R E A D ; 
READS:                               R E A D S ; 
REFERENCES:                          R E F E R E N C E S ; 
REGEXP:                              R E G E X P ; 
RELEASE:                             R E L E A S E ; 
RENAME:                              R E N A M E ; 
REPEAT:                              R E P E A T ; 
REPLACE:                             R E P L A C E ; 
REQUIRE:                             R E Q U I R E ; 
RESTRICT:                            R E S T R I C T ; 
RETURN:                              R E T U R N ; 
REVOKE:                              R E V O K E ; 
RIGHT:                               R I G H T ; 
RLIKE:                               R L I K E ; 
SCHEMA:                              S C H E M A ; 
SCHEMAS:                             S C H E M A S ; 
SELECT:                              S E L E C T ; 
SET:                                 S E T ; 
SEPARATOR:                           S E P A R A T O R ; 
SHOW:                                S H O W ; 
SPATIAL:                             S P A T I A L ; 
SQL:                                 S Q L ; 
SQLEXCEPTION:                        S Q L E X C E P T I O N ; 
SQLSTATE:                            S Q L S T A T E ; 
SQLWARNING:                          S Q L W A R N I N G ; 
SQL_BIG_RESULT:                      S Q L '_' B I G '_' R E S U L T ; 
SQL_CALC_FOUND_ROWS:                 S Q L '_' C A L C '_' F O U N D '_' R O W S ; 
SQL_SMALL_RESULT:                    S Q L '_' S M A L L '_' R E S U L T ; 
SSL:                                 S S L ; 
STARTING:                            S T A R T I N G ; 
STRAIGHT_JOIN:                       S T R A I G H T '_' J O I N ; 
TABLE:                               T A B L E ; 
TERMINATED:                          T E R M I N A T E D ; 
THEN:                                T H E N ; 
TO:                                  T O ; 
TRAILING:                            T R A I L I N G ; 
TRIGGER:                             T R I G G E R ; 
TRUE:                                T R U E ; 
UNDO:                                U N D O ; 
UNION:                               U N I O N ; 
UNIQUE:                              U N I Q U E ; 
UNLOCK:                              U N L O C K ; 
UNSIGNED:                            U N S I G N E D ; 
UPDATE:                              U P D A T E ; 
USAGE:                               U S A G E ; 
USE:                                 U S E ; 
USING:                               U S I N G ; 
VALUES:                              V A L U E S ; 
WHEN:                                W H E N ; 
WHERE:                               W H E R E ; 
WHILE:                               W H I L E ; 
WITH:                                W I T H ; 
WRITE:                               W R I T E ; 
XOR:                                 X O R ; 
ZEROFILL:                            Z E R O F I L L ; 


// DATA TYPE Keywords

TINYINT:                             T I N Y I N T ; 
SMALLINT:                            S M A L L I N T ; 
MEDIUMINT:                           M E D I U M I N T ; 
INT:                                 I N T ; 
INTEGER:                             I N T E G E R ; 
BIGINT:                              B I G I N T ; 
REAL:                                R E A L ; 
DOUBLE:                              D O U B L E ; 
FLOAT:                               F L O A T ; 
DECIMAL:                             D E C I M A L ; 
NUMERIC:                             N U M E R I C ; 
DATE:                                D A T E ; 
TIME:                                T I M E ; 
TIMESTAMP:                           T I M E S T A M P ; 
DATETIME:                            D A T E T I M E ; 
YEAR:                                Y E A R ; 
CHAR:                                C H A R ; 
VARCHAR:                             V A R C H A R ; 
BINARY:                              B I N A R Y ; 
VARBINARY:                           V A R B I N A R Y ; 
TINYBLOB:                            T I N Y B L O B ; 
BLOB:                                B L O B ; 
MEDIUMBLOB:                          M E D I U M B L O B ; 
LONGBLOB:                            L O N G B L O B ; 
TINYTEXT:                            T I N Y T E X T ; 
TEXT:                                T E X T ; 
MEDIUMTEXT:                          M E D I U M T E X T ; 
LONGTEXT:                            L O N G T E X T ; 
ENUM:                                E N U M ; 


// Interval type Keywords

YEAR_MONTH:                          Y E A R '_' M O N T H ; 
DAY_HOUR:                            D A Y '_' H O U R ; 
DAY_MINUTE:                          D A Y '_' M I N U T E ; 
DAY_SECOND:                          D A Y '_' S E C O N D ; 
HOUR_MINUTE:                         H O U R '_' M I N U T E ; 
HOUR_SECOND:                         H O U R '_' S E C O N D ; 
MINUTE_SECOND:                       M I N U T E '_' S E C O N D ; 
SECOND_MICROSECOND:                  S E C O N D '_' M I C R O S E C O N D ; 
MINUTE_MICROSECOND:                  M I N U T E '_' M I C R O S E C O N D ; 
HOUR_MICROSECOND:                    H O U R '_' M I C R O S E C O N D ; 
DAY_MICROSECOND:                     D A Y '_' M I C R O S E C O N D ; 


// Group function Keywords

AVG:                                 A V G ; 
BIT_AND:                             B I T '_' A N D ; 
BIT_OR:                              B I T '_' O R ; 
BIT_XOR:                             B I T '_' X O R ; 
COUNT:                               C O U N T ; 
GROUP_CONCAT:                        G R O U P '_' C O N C A T ; 
MAX:                                 M A X ; 
MIN:                                 M I N ; 
STD:                                 S T D ; 
STDDEV:                              S T D D E V ; 
STDDEV_POP:                          S T D D E V '_' P O P ; 
STDDEV_SAMP:                         S T D D E V '_' S A M P ; 
SUM:                                 S U M ; 
VAR_POP:                             V A R '_' P O P ; 
VAR_SAMP:                            V A R '_' S A M P ; 
VARIANCE:                            V A R I A N C E ; 


// Common function Keywords

CURRENT_DATE:                        C U R R E N T '_' D A T E ; 
CURRENT_TIME:                        C U R R E N T '_' T I M E ; 
CURRENT_TIMESTAMP:                   C U R R E N T '_' T I M E S T A M P ; 
LOCALTIME:                           L O C A L T I M E ; 
CURDATE:                             C U R D A T E ; 
CURTIME:                             C U R T I M E ; 
DATE_ADD:                            D A T E '_' A D D ; 
DATE_SUB:                            D A T E '_' S U B ; 
EXTRACT:                             E X T R A C T ; 
LOCALTIMESTAMP:                      L O C A L T I M E S T A M P ; 
NOW:                                 N O W ; 
POSITION:                            P O S I T I O N ; 
SUBSTR:                              S U B S T R ; 
SUBSTRING:                           S U B S T R I N G ; 
SYSDATE:                             S Y S D A T E ; 
TRIM:                                T R I M ; 
UTC_DATE:                            U T C '_' D A T E ; 
UTC_TIME:                            U T C '_' T I M E ; 
UTC_TIMESTAMP:                       U T C '_' T I M E S T A M P ; 



// Keywords, but can be ID
// Common Keywords, but can be ID

ACCOUNT:                             A C C O U N T ; 
ACTION:                              A C T I O N ; 
AFTER:                               A F T E R ; 
AGGREGATE:                           A G G R E G A T E ; 
ALGORITHM:                           A L G O R I T H M ; 
ANY:                                 A N Y ; 
AT:                                  A T ; 
AUTHORS:                             A U T H O R S ; 
AUTOCOMMIT:                          A U T O C O M M I T ; 
AUTOEXTEND_SIZE:                     A U T O E X T E N D '_' S I Z E ; 
AUTO_INCREMENT:                      A U T O '_' I N C R E M E N T ; 
AVG_ROW_LENGTH:                      A V G '_' R O W '_' L E N G T H ; 
BEGIN:                               B E G I N ; 
BINLOG:                              B I N L O G ; 
BIT:                                 B I T ; 
BLOCK:                               B L O C K ; 
BOOL:                                B O O L ; 
BOOLEAN:                             B O O L E A N ; 
BTREE:                               B T R E E ; 
CACHE:                               C A C H E ; 
CASCADED:                            C A S C A D E D ; 
CHAIN:                               C H A I N ; 
CHANGED:                             C H A N G E D ; 
CHANNEL:                             C H A N N E L ; 
CHECKSUM:                            C H E C K S U M ; 
CIPHER:                              C I P H E R ; 
CLIENT:                              C L I E N T ; 
CLOSE:                               C L O S E ; 
COALESCE:                            C O A L E S C E ; 
CODE:                                C O D E ; 
COLUMNS:                             C O L U M N S ; 
COLUMN_FORMAT:                       C O L U M N '_' F O R M A T ; 
COMMENT:                             C O M M E N T ; 
COMMIT:                              C O M M I T ; 
COMPACT:                             C O M P A C T ; 
COMPLETION:                          C O M P L E T I O N ; 
COMPRESSED:                          C O M P R E S S E D ; 
COMPRESSION:                         C O M P R E S S I O N ; 
CONCURRENT:                          C O N C U R R E N T ; 
CONNECTION:                          C O N N E C T I O N ; 
CONSISTENT:                          C O N S I S T E N T ; 
CONTAINS:                            C O N T A I N S ; 
CONTEXT:                             C O N T E X T ; 
CONTRIBUTORS:                        C O N T R I B U T O R S ; 
COPY:                                C O P Y ; 
CPU:                                 C P U ; 
DATA:                                D A T A ; 
DATAFILE:                            D A T A F I L E ; 
DEALLOCATE:                          D E A L L O C A T E ; 
DEFAULT_AUTH:                        D E F A U L T '_' A U T H ; 
DEFINER:                             D E F I N E R ; 
DELAY_KEY_WRITE:                     D E L A Y '_' K E Y '_' W R I T E ; 
DES_KEY_FILE:                        D E S '_' K E Y '_' F I L E ; 
DIRECTORY:                           D I R E C T O R Y ; 
DISABLE:                             D I S A B L E ; 
DISCARD:                             D I S C A R D ; 
DISK:                                D I S K ; 
DO:                                  D O ; 
DUMPFILE:                            D U M P F I L E ; 
DUPLICATE:                           D U P L I C A T E ; 
DYNAMIC:                             D Y N A M I C ; 
ENABLE:                              E N A B L E ; 
ENCRYPTION:                          E N C R Y P T I O N ; 
END:                                 E N D ; 
ENDS:                                E N D S ; 
ENGINE:                              E N G I N E ; 
ENGINES:                             E N G I N E S ; 
ERROR:                               E R R O R ; 
ERRORS:                              E R R O R S ; 
ESCAPE:                              E S C A P E ; 
EVEN:                                E V E N ; 
EVENT:                               E V E N T ; 
EVENTS:                              E V E N T S ; 
EVERY:                               E V E R Y ; 
EXCHANGE:                            E X C H A N G E ; 
EXCLUSIVE:                           E X C L U S I V E ; 
EXPIRE:                              E X P I R E ; 
EXPORT:                              E X P O R T ; 
EXTENDED:                            E X T E N D E D ; 
EXTENT_SIZE:                         E X T E N T '_' S I Z E ; 
FAST:                                F A S T ; 
FAULTS:                              F A U L T S ; 
FIELDS:                              F I E L D S ; 
FILE_BLOCK_SIZE:                     F I L E '_' B L O C K '_' S I Z E ; 
FILTER:                              F I L T E R ; 
FIRST:                               F I R S T ; 
FIXED:                               F I X E D ; 
FLUSH:                               F L U S H ; 
FOLLOWS:                             F O L L O W S ; 
FOUND:                               F O U N D ; 
FULL:                                F U L L ; 
FUNCTION:                            F U N C T I O N ; 
GENERAL:                             G E N E R A L ; 
GLOBAL:                              G L O B A L ; 
GRANTS:                              G R A N T S ; 
GROUP_REPLICATION:                   G R O U P '_' R E P L I C A T I O N ; 
HANDLER:                             H A N D L E R ; 
HASH:                                H A S H ; 
HELP:                                H E L P ; 
HOST:                                H O S T ; 
HOSTS:                               H O S T S ; 
IDENTIFIED:                          I D E N T I F I E D ; 
IGNORE_SERVER_IDS:                   I G N O R E '_' S E R V E R '_' I D S ; 
IMPORT:                              I M P O R T ; 
INDEXES:                             I N D E X E S ; 
INITIAL_SIZE:                        I N I T I A L '_' S I Z E ; 
INPLACE:                             I N P L A C E ; 
INSERT_METHOD:                       I N S E R T '_' M E T H O D ; 
INSTALL:                             I N S T A L L ; 
INSTANCE:                            I N S T A N C E ; 
INVOKER:                             I N V O K E R ; 
IO:                                  I O ; 
IO_THREAD:                           I O '_' T H R E A D ; 
IPC:                                 I P C ; 
ISOLATION:                           I S O L A T I O N ; 
ISSUER:                              I S S U E R ; 
JSON:                                J S O N ; 
KEY_BLOCK_SIZE:                      K E Y '_' B L O C K '_' S I Z E ; 
LANGUAGE:                            L A N G U A G E ; 
LAST:                                L A S T ; 
LEAVES:                              L E A V E S ; 
LESS:                                L E S S ; 
LEVEL:                               L E V E L ; 
LIST:                                L I S T ; 
LOCAL:                               L O C A L ; 
LOGFILE:                             L O G F I L E ; 
LOGS:                                L O G S ; 
MASTER:                              M A S T E R ; 
MASTER_AUTO_POSITION:                M A S T E R '_' A U T O '_' P O S I T I O N ; 
MASTER_CONNECT_RETRY:                M A S T E R '_' C O N N E C T '_' R E T R Y ; 
MASTER_DELAY:                        M A S T E R '_' D E L A Y ; 
MASTER_HEARTBEAT_PERIOD:             M A S T E R '_' H E A R T B E A T '_' P E R I O D ; 
MASTER_HOST:                         M A S T E R '_' H O S T ; 
MASTER_LOG_FILE:                     M A S T E R '_' L O G '_' F I L E ; 
MASTER_LOG_POS:                      M A S T E R '_' L O G '_' P O S ; 
MASTER_PASSWORD:                     M A S T E R '_' P A S S W O R D ; 
MASTER_PORT:                         M A S T E R '_' P O R T ; 
MASTER_RETRY_COUNT:                  M A S T E R '_' R E T R Y '_' C O U N T ; 
MASTER_SSL:                          M A S T E R '_' S S L ; 
MASTER_SSL_CA:                       M A S T E R '_' S S L '_' C A ; 
MASTER_SSL_CAPATH:                   M A S T E R '_' S S L '_' C A P A T H ; 
MASTER_SSL_CERT:                     M A S T E R '_' S S L '_' C E R T ; 
MASTER_SSL_CIPHER:                   M A S T E R '_' S S L '_' C I P H E R ; 
MASTER_SSL_CRL:                      M A S T E R '_' S S L '_' C R L ; 
MASTER_SSL_CRLPATH:                  M A S T E R '_' S S L '_' C R L P A T H ; 
MASTER_SSL_KEY:                      M A S T E R '_' S S L '_' K E Y ; 
MASTER_TLS_VERSION:                  M A S T E R '_' T L S '_' V E R S I O N ; 
MASTER_USER:                         M A S T E R '_' U S E R ; 
MAX_CONNECTIONS_PER_HOUR:            M A X '_' C O N N E C T I O N S '_' P E R '_' H O U R ; 
MAX_QUERIES_PER_HOUR:                M A X '_' Q U E R I E S '_' P E R '_' H O U R ; 
MAX_ROWS:                            M A X '_' R O W S ; 
MAX_SIZE:                            M A X '_' S I Z E ; 
MAX_UPDATES_PER_HOUR:                M A X '_' U P D A T E S '_' P E R '_' H O U R ; 
MAX_USER_CONNECTIONS:                M A X '_' U S E R '_' C O N N E C T I O N S ; 
MEDIUM:                              M E D I U M ; 
MERGE:                               M E R G E ; 
MID:                                 M I D ; 
MIGRATE:                             M I G R A T E ; 
MIN_ROWS:                            M I N '_' R O W S ; 
MODE:                                M O D E ; 
MODIFY:                              M O D I F Y ; 
MUTEX:                               M U T E X ; 
MYSQL:                               M Y S Q L ; 
NAME:                                N A M E ; 
NAMES:                               N A M E S ; 
NCHAR:                               N C H A R ; 
NEVER:                               N E V E R ; 
NEXT:                                N E X T ; 
NO:                                  N O ; 
NODEGROUP:                           N O D E G R O U P ; 
NONE:                                N O N E ; 
OFFLINE:                             O F F L I N E ; 
OFFSET:                              O F F S E T ; 
OJ:                                  O J ; 
OLD_PASSWORD:                        O L D '_' P A S S W O R D ; 
ONE:                                 O N E ; 
ONLINE:                              O N L I N E ; 
ONLY:                                O N L Y ; 
OPEN:                                O P E N ; 
OPTIMIZER_COSTS:                     O P T I M I Z E R '_' C O S T S ; 
OPTIONS:                             O P T I O N S ; 
OWNER:                               O W N E R ; 
PACK_KEYS:                           P A C K '_' K E Y S ; 
PAGE:                                P A G E ; 
PARSER:                              P A R S E R ; 
PARTIAL:                             P A R T I A L ; 
PARTITIONING:                        P A R T I T I O N I N G ; 
PARTITIONS:                          P A R T I T I O N S ; 
PASSWORD:                            P A S S W O R D ; 
PHASE:                               P H A S E ; 
PLUGIN:                              P L U G I N ; 
PLUGIN_DIR:                          P L U G I N '_' D I R ; 
PLUGINS:                             P L U G I N S ; 
PORT:                                P O R T ; 
PRECEDES:                            P R E C E D E S ; 
PREPARE:                             P R E P A R E ; 
PRESERVE:                            P R E S E R V E ; 
PREV:                                P R E V ; 
PROCESSLIST:                         P R O C E S S L I S T ; 
PROFILE:                             P R O F I L E ; 
PROFILES:                            P R O F I L E S ; 
PROXY:                               P R O X Y ; 
QUERY:                               Q U E R Y ; 
QUICK:                               Q U I C K ; 
REBUILD:                             R E B U I L D ; 
RECOVER:                             R E C O V E R ; 
REDO_BUFFER_SIZE:                    R E D O '_' B U F F E R '_' S I Z E ; 
REDUNDANT:                           R E D U N D A N T ; 
RELAY:                               R E L A Y ; 
RELAY_LOG_FILE:                      R E L A Y '_' L O G '_' F I L E ; 
RELAY_LOG_POS:                       R E L A Y '_' L O G '_' P O S ; 
RELAYLOG:                            R E L A Y L O G ; 
REMOVE:                              R E M O V E ; 
REORGANIZE:                          R E O R G A N I Z E ; 
REPAIR:                              R E P A I R ; 
REPLICATE_DO_DB:                     R E P L I C A T E '_' D O '_' D B ; 
REPLICATE_DO_TABLE:                  R E P L I C A T E '_' D O '_' T A B L E ; 
REPLICATE_IGNORE_DB:                 R E P L I C A T E '_' I G N O R E '_' D B ; 
REPLICATE_IGNORE_TABLE:              R E P L I C A T E '_' I G N O R E '_' T A B L E ; 
REPLICATE_REWRITE_DB:                R E P L I C A T E '_' R E W R I T E '_' D B ; 
REPLICATE_WILD_DO_TABLE:             R E P L I C A T E '_' W I L D '_' D O '_' T A B L E ; 
REPLICATE_WILD_IGNORE_TABLE:         R E P L I C A T E '_' W I L D '_' I G N O R E '_' T A B L E ;
REPLICATION:                         R E P L I C A T I O N ; 
RESET:                               R E S E T ; 
RESUME:                              R E S U M E ; 
RETURNS:                             R E T U R N S ; 
ROLLBACK:                            R O L L B A C K ; 
ROLLUP:                              R O L L U P ; 
ROTATE:                              R O T A T E ; 
ROW:                                 R O W ; 
ROWS:                                R O W S ; 
ROW_FORMAT:                          R O W '_' F O R M A T ; 
SAVEPOINT:                           S A V E P O I N T ; 
SCHEDULE:                            S C H E D U L E ; 
SECURITY:                            S E C U R I T Y ; 
SERVER:                              S E R V E R ; 
SESSION:                             S E S S I O N ; 
SHARE:                               S H A R E ; 
SHARED:                              S H A R E D ; 
SIGNED:                              S I G N E D ; 
SIMPLE:                              S I M P L E ; 
SLAVE:                               S L A V E ; 
SLOW:                                S L O W ; 
SNAPSHOT:                            S N A P S H O T ; 
SOCKET:                              S O C K E T ; 
SOME:                                S O M E ; 
SONAME:                              S O N A M E ; 
SOUNDS:                              S O U N D S ; 
SOURCE:                              S O U R C E ; 
SQL_AFTER_GTIDS:                     S Q L '_' A F T E R '_' G T I D S ; 
SQL_AFTER_MTS_GAPS:                  S Q L '_' A F T E R '_' M T S '_' G A P S ; 
SQL_BEFORE_GTIDS:                    S Q L '_' B E F O R E '_' G T I D S ; 
SQL_BUFFER_RESULT:                   S Q L '_' B U F F E R '_' R E S U L T ; 
SQL_CACHE:                           S Q L '_' C A C H E ; 
SQL_NO_CACHE:                        S Q L '_' N O '_' C A C H E ; 
SQL_THREAD:                          S Q L '_' T H R E A D ; 
START:                               S T A R T ; 
STARTS:                              S T A R T S ; 
STATS_AUTO_RECALC:                   S T A T S '_' A U T O '_' R E C A L C ; 
STATS_PERSISTENT:                    S T A T S '_' P E R S I S T E N T ; 
STATS_SAMPLE_PAGES:                  S T A T S '_' S A M P L E '_' P A G E S ; 
STATUS:                              S T A T U S ; 
STOP:                                S T O P ; 
STORAGE:                             S T O R A G E ; 
STRING:                              S T R I N G ; 
SUBJECT:                             S U B J E C T ; 
SUBPARTITION:                        S U B P A R T I T I O N ; 
SUBPARTITIONS:                       S U B P A R T I T I O N S ; 
SUSPEND:                             S U S P E N D ; 
SWAPS:                               S W A P S ; 
SWITCHES:                            S W I T C H E S ; 
TABLESPACE:                          T A B L E S P A C E ; 
TEMPORARY:                           T E M P O R A R Y ; 
TEMPTABLE:                           T E M P T A B L E ; 
THAN:                                T H A N ; 
TRADITIONAL:                         T R A D I T I O N A L ; 
TRANSACTION:                         T R A N S A C T I O N ; 
TRIGGERS:                            T R I G G E R S ; 
TRUNCATE:                            T R U N C A T E ; 
UNDEFINED:                           U N D E F I N E D ; 
UNDOFILE:                            U N D O F I L E ; 
UNDO_BUFFER_SIZE:                    U N D O '_' B U F F E R '_' S I Z E ; 
UNINSTALL:                           U N I N S T A L L ; 
UNKNOWN:                             U N K N O W N ; 
UNTIL:                               U N T I L ; 
UPGRADE:                             U P G R A D E ; 
USER:                                U S E R ; 
USE_FRM:                             U S E '_' F R M ; 
USER_RESOURCES:                      U S E R '_' R E S O U R C E S ; 
VALIDATION:                          V A L I D A T I O N ; 
VALUE:                               V A L U E ; 
VARIABLES:                           V A R I A B L E S ; 
VIEW:                                V I E W ; 
WAIT:                                W A I T ; 
WARNINGS:                            W A R N I N G S ; 
WITHOUT:                             W I T H O U T ; 
WORK:                                W O R K ; 
WRAPPER:                             W R A P P E R ; 
X509:                                X '5' '0' '9' ; 
XA:                                  X A ; 
XML:                                 X M L ; 


// Date format Keywords

EUR:                                 E U R ; 
USA:                                 U S A ; 
JIS:                                 J I S ; 
ISO:                                 I S O ; 
INTERNAL:                            I N T E R N A L ; 


// Interval type Keywords

QUARTER:                             Q U A R T E R ; 
MONTH:                               M O N T H ; 
DAY:                                 D A Y ; 
HOUR:                                H O U R ; 
MINUTE:                              M I N U T E ; 
WEEK:                                W E E K ; 
SECOND:                              S E C O N D ; 
MICROSECOND:                         M I C R O S E C O N D ; 


// PRIVILEGES

TABLES:                              T A B L E S ; 
ROUTINE:                             R O U T I N E ; 
EXECUTE:                             E X E C U T E ; 
FILE:                                F I L E ; 
PROCESS:                             P R O C E S S ; 
RELOAD:                              R E L O A D ; 
SHUTDOWN:                            S H U T D O W N ; 
SUPER:                               S U P E R ; 
PRIVILEGES:                          P R I V I L E G E S ; 


// Charsets

ARMSCII8:                            A R M S C I I '8' ; 
ASCII:                               A S C I I ; 
BIG5:                                B I G '5' ; 
CP1250:                              C P '1' '2' '5' '0' ; 
CP1251:                              C P '1' '2' '5' '1' ; 
CP1256:                              C P '1' '2' '5' '6' ; 
CP1257:                              C P '1' '2' '5' '7' ; 
CP850:                               C P '8' '5' '0' ; 
CP852:                               C P '8' '5' '2' ; 
CP866:                               C P '8' '6' '6' ; 
CP932:                               C P '9' '3' '2' ; 
DEC8:                                D E C '8' ; 
EUCJPMS:                             E U C J P M S ; 
EUCKR:                               E U C K R ; 
GB2312:                              G B '2' '3' '1' '2' ; 
GBK:                                 G B K ; 
GEOSTD8:                             G E O S T D '8' ; 
GREEK:                               G R E E K ; 
HEBREW:                              H E B R E W ; 
HP8:                                 H P '8' ; 
KEYBCS2:                             K E Y B C S '2' ; 
KOI8R:                               K O I '8' R ; 
KOI8U:                               K O I '8' U ; 
LATIN1:                              L A T I N '1' ; 
LATIN2:                              L A T I N '2' ; 
LATIN5:                              L A T I N '5' ; 
LATIN7:                              L A T I N '7' ; 
MACCE:                               M A C C E ; 
MACROMAN:                            M A C R O M A N ; 
SJIS:                                S J I S ; 
SWE7:                                S W E '7' ; 
TIS620:                              T I S '6' '2' '0' ; 
UCS2:                                U C S '2' ; 
UJIS:                                U J I S ; 
UTF16:                               U T F '1' '6' ; 
UTF16LE:                             U T F '1' '6' L E ; 
UTF32:                               U T F '3' '2' ; 
UTF8:                                U T F '8' ; 
UTF8MB3:                             U T F '8' M B '3' ; 
UTF8MB4:                             U T F '8' M B '4' ; 


// DB Engines

ARCHIVE:                             A R C H I V E ; 
BLACKHOLE:                           B L A C K H O L E ; 
CSV:                                 C S V ; 
FEDERATED:                           F E D E R A T E D ; 
INNODB:                              I N N O D B ; 
MEMORY:                              M E M O R Y ; 
MRG_MYISAM:                          M R G '_' M Y I S A M ; 
MYISAM:                              M Y I S A M ; 
NDB:                                 N D B ; 
NDBCLUSTER:                          N D B C L U S T E R ; 
PERFOMANCE_SCHEMA:                   P E R F O M A N C E '_' S C H E M A ; 


// Transaction Levels

REPEATABLE:                          R E P E A T A B L E ; 
COMMITTED:                           C O M M I T T E D ; 
UNCOMMITTED:                         U N C O M M I T T E D ; 
SERIALIZABLE:                        S E R I A L I Z A B L E ; 


// Spatial data types

GEOMETRYCOLLECTION:                  G E O M E T R Y C O L L E C T I O N ; 
LINESTRING:                          L I N E S T R I N G ; 
MULTILINESTRING:                     M U L T I L I N E S T R I N G ; 
MULTIPOINT:                          M U L T I P O I N T ; 
MULTIPOLYGON:                        M U L T I P O L Y G O N ; 
POINT:                               P O I N T ; 
POLYGON:                             P O L Y G O N ; 


// Common function names

ABS:                                 A B S ; 
ACOS:                                A C O S ; 
ADDDATE:                             A D D D A T E ; 
ADDTIME:                             A D D T I M E ; 
AES_DECRYPT:                         A E S '_' D E C R Y P T ; 
AES_ENCRYPT:                         A E S '_' E N C R Y P T ; 
AREA:                                A R E A ; 
ASBINARY:                            A S B I N A R Y ; 
ASIN:                                A S I N ; 
ASTEXT:                              A S T E X T ; 
ASWKB:                               A S W K B ; 
ASWKT:                               A S W K T ; 
ASYMMETRIC_DECRYPT:                  A S Y M M E T R I C '_' D E C R Y P T ; 
ASYMMETRIC_DERIVE:                   A S Y M M E T R I C '_' D E R I V E ; 
ASYMMETRIC_ENCRYPT:                  A S Y M M E T R I C '_' E N C R Y P T ; 
ASYMMETRIC_SIGN:                     A S Y M M E T R I C '_' S I G N ; 
ASYMMETRIC_VERIFY:                   A S Y M M E T R I C '_' V E R I F Y ; 
ATAN:                                A T A N ; 
ATAN2:                               A T A N '2' ; 
BENCHMARK:                           B E N C H M A R K ; 
BIN:                                 B I N ; 
BIT_COUNT:                           B I T '_' C O U N T ; 
BIT_LENGTH:                          B I T '_' L E N G T H ; 
BUFFER:                              B U F F E R ; 
CEIL:                                C E I L ; 
CEILING:                             C E I L I N G ; 
CENTROID:                            C E N T R O I D ; 
CHARACTER_LENGTH:                    C H A R A C T E R '_' L E N G T H ; 
CHARSET:                             C H A R S E T ; 
CHAR_LENGTH:                         C H A R '_' L E N G T H ; 
COERCIBILITY:                        C O E R C I B I L I T Y ; 
COLLATION:                           C O L L A T I O N ; 
COMPRESS:                            C O M P R E S S ; 
CONCAT:                              C O N C A T ; 
CONCAT_WS:                           C O N C A T '_' W S ; 
CONNECTION_ID:                       C O N N E C T I O N '_' I D ; 
CONV:                                C O N V ; 
CONVERT_TZ:                          C O N V E R T '_' T Z ; 
COS:                                 C O S ; 
COT:                                 C O T ; 
CRC32:                               C R C '3' '2' ; 
CREATE_ASYMMETRIC_PRIV_KEY:          C R E A T E '_' A S Y M M E T R I C '_' P R I V '_' K E Y ;
CREATE_ASYMMETRIC_PUB_KEY:           C R E A T E '_' A S Y M M E T R I C '_' P U B '_' K E Y ;
CREATE_DH_PARAMETERS:                C R E A T E '_' D H '_' P A R A M E T E R S ; 
CREATE_DIGEST:                       C R E A T E '_' D I G E S T ; 
CROSSES:                             C R O S S E S ; 
DATEDIFF:                            D A T E D I F F ; 
DATE_FORMAT:                         D A T E '_' F O R M A T ; 
DAYNAME:                             D A Y N A M E ; 
DAYOFMONTH:                          D A Y O F M O N T H ; 
DAYOFWEEK:                           D A Y O F W E E K ; 
DAYOFYEAR:                           D A Y O F Y E A R ; 
DECODE:                              D E C O D E ; 
DEGREES:                             D E G R E E S ; 
DES_DECRYPT:                         D E S '_' D E C R Y P T ; 
DES_ENCRYPT:                         D E S '_' E N C R Y P T ; 
DIMENSION:                           D I M E N S I O N ; 
DISJOINT:                            D I S J O I N T ; 
ELT:                                 E L T ; 
ENCODE:                              E N C O D E ; 
ENCRYPT:                             E N C R Y P T ; 
ENDPOINT:                            E N D P O I N T ; 
ENVELOPE:                            E N V E L O P E ; 
EQUALS:                              E Q U A L S ; 
EXP:                                 E X P ; 
EXPORT_SET:                          E X P O R T '_' S E T ; 
EXTERIORRING:                        E X T E R I O R R I N G ; 
EXTRACTVALUE:                        E X T R A C T V A L U E ; 
FIELD:                               F I E L D ; 
FIND_IN_SET:                         F I N D '_' I N '_' S E T ; 
FLOOR:                               F L O O R ; 
FORMAT:                              F O R M A T ; 
FOUND_ROWS:                          F O U N D '_' R O W S ; 
FROM_BASE64:                         F R O M '_' B A S E '6' '4' ; 
FROM_DAYS:                           F R O M '_' D A Y S ; 
FROM_UNIXTIME:                       F R O M '_' U N I X T I M E ; 
GEOMCOLLFROMTEXT:                    G E O M C O L L F R O M T E X T ; 
GEOMCOLLFROMWKB:                     G E O M C O L L F R O M W K B ; 
GEOMETRYCOLLECTIONFROMTEXT:          G E O M E T R Y C O L L E C T I O N F R O M T E X T ;
GEOMETRYCOLLECTIONFROMWKB:           G E O M E T R Y C O L L E C T I O N F R O M W K B ;
GEOMETRYFROMTEXT:                    G E O M E T R Y F R O M T E X T ; 
GEOMETRYFROMWKB:                     G E O M E T R Y F R O M W K B ; 
GEOMETRYN:                           G E O M E T R Y N ; 
GEOMETRYTYPE:                        G E O M E T R Y T Y P E ; 
GEOMFROMTEXT:                        G E O M F R O M T E X T ; 
GEOMFROMWKB:                         G E O M F R O M W K B ; 
GET_FORMAT:                          G E T '_' F O R M A T ; 
GET_LOCK:                            G E T '_' L O C K ; 
GLENGTH:                             G L E N G T H ; 
GREATEST:                            G R E A T E S T ; 
GTID_SUBSET:                         G T I D '_' S U B S E T ; 
GTID_SUBTRACT:                       G T I D '_' S U B T R A C T ; 
HEX:                                 H E X ; 
IFNULL:                              I F N U L L ; 
INET6_ATON:                          I N E T '6' '_' A T O N ; 
INET6_NTOA:                          I N E T '6' '_' N T O A ; 
INET_ATON:                           I N E T '_' A T O N ; 
INET_NTOA:                           I N E T '_' N T O A ; 
INSTR:                               I N S T R ; 
INTERIORRINGN:                       I N T E R I O R R I N G N ; 
INTERSECTS:                          I N T E R S E C T S ; 
ISCLOSED:                            I S C L O S E D ; 
ISEMPTY:                             I S E M P T Y ; 
ISNULL:                              I S N U L L ; 
ISSIMPLE:                            I S S I M P L E ; 
IS_FREE_LOCK:                        I S '_' F R E E '_' L O C K ; 
IS_IPV4:                             I S '_' I P V '4' ; 
IS_IPV4_COMPAT:                      I S '_' I P V '4' '_' C O M P A T ; 
IS_IPV4_MAPPED:                      I S '_' I P V '4' '_' M A P P E D ; 
IS_IPV6:                             I S '_' I P V '6' ; 
IS_USED_LOCK:                        I S '_' U S E D '_' L O C K ; 
LAST_INSERT_ID:                      L A S T '_' I N S E R T '_' I D ; 
LCASE:                               L C A S E ; 
LEAST:                               L E A S T ; 
LENGTH:                              L E N G T H ; 
LINEFROMTEXT:                        L I N E F R O M T E X T ; 
LINEFROMWKB:                         L I N E F R O M W K B ; 
LINESTRINGFROMTEXT:                  L I N E S T R I N G F R O M T E X T ; 
LINESTRINGFROMWKB:                   L I N E S T R I N G F R O M W K B ; 
LN:                                  L N ; 
LOAD_FILE:                           L O A D '_' F I L E ; 
LOCATE:                              L O C A T E ; 
LOG:                                 L O G ; 
LOG10:                               L O G '1' '0' ; 
LOG2:                                L O G '2' ; 
LOWER:                               L O W E R ; 
LPAD:                                L P A D ; 
LTRIM:                               L T R I M ; 
MAKEDATE:                            M A K E D A T E ; 
MAKETIME:                            M A K E T I M E ; 
MAKE_SET:                            M A K E '_' S E T ; 
MASTER_POS_WAIT:                     M A S T E R '_' P O S '_' W A I T ; 
MBRCONTAINS:                         M B R C O N T A I N S ; 
MBRDISJOINT:                         M B R D I S J O I N T ; 
MBREQUAL:                            M B R E Q U A L ; 
MBRINTERSECTS:                       M B R I N T E R S E C T S ; 
MBROVERLAPS:                         M B R O V E R L A P S ; 
MBRTOUCHES:                          M B R T O U C H E S ; 
MBRWITHIN:                           M B R W I T H I N ; 
MD5:                                 M D '5' ; 
MLINEFROMTEXT:                       M L I N E F R O M T E X T ; 
MLINEFROMWKB:                        M L I N E F R O M W K B ; 
MONTHNAME:                           M O N T H N A M E ; 
MPOINTFROMTEXT:                      M P O I N T F R O M T E X T ; 
MPOINTFROMWKB:                       M P O I N T F R O M W K B ; 
MPOLYFROMTEXT:                       M P O L Y F R O M T E X T ; 
MPOLYFROMWKB:                        M P O L Y F R O M W K B ; 
MULTILINESTRINGFROMTEXT:             M U L T I L I N E S T R I N G F R O M T E X T ; 
MULTILINESTRINGFROMWKB:              M U L T I L I N E S T R I N G F R O M W K B ; 
MULTIPOINTFROMTEXT:                  M U L T I P O I N T F R O M T E X T ; 
MULTIPOINTFROMWKB:                   M U L T I P O I N T F R O M W K B ; 
MULTIPOLYGONFROMTEXT:                M U L T I P O L Y G O N F R O M T E X T ; 
MULTIPOLYGONFROMWKB:                 M U L T I P O L Y G O N F R O M W K B ; 
NAME_CONST:                          N A M E '_' C O N S T ; 
NULLIF:                              N U L L I F ; 
NUMGEOMETRIES:                       N U M G E O M E T R I E S ; 
NUMINTERIORRINGS:                    N U M I N T E R I O R R I N G S ; 
NUMPOINTS:                           N U M P O I N T S ; 
OCT:                                 O C T ; 
OCTET_LENGTH:                        O C T E T '_' L E N G T H ; 
ORD:                                 O R D ; 
OVERLAPS:                            O V E R L A P S ; 
PERIOD_ADD:                          P E R I O D '_' A D D ; 
PERIOD_DIFF:                         P E R I O D '_' D I F F ; 
PI:                                  P I ; 
POINTFROMTEXT:                       P O I N T F R O M T E X T ; 
POINTFROMWKB:                        P O I N T F R O M W K B ; 
POINTN:                              P O I N T N ; 
POLYFROMTEXT:                        P O L Y F R O M T E X T ; 
POLYFROMWKB:                         P O L Y F R O M W K B ; 
POLYGONFROMTEXT:                     P O L Y G O N F R O M T E X T ; 
POLYGONFROMWKB:                      P O L Y G O N F R O M W K B ; 
POW:                                 P O W ; 
POWER:                               P O W E R ; 
QUOTE:                               Q U O T E ; 
RADIANS:                             R A D I A N S ; 
RAND:                                R A N D ; 
RANDOM_BYTES:                        R A N D O M '_' B Y T E S ; 
RELEASE_LOCK:                        R E L E A S E '_' L O C K ; 
REVERSE:                             R E V E R S E ; 
ROUND:                               R O U N D ; 
ROW_COUNT:                           R O W '_' C O U N T ; 
RPAD:                                R P A D ; 
RTRIM:                               R T R I M ; 
SEC_TO_TIME:                         S E C '_' T O '_' T I M E ; 
SESSION_USER:                        S E S S I O N '_' U S E R ; 
SHA:                                 S H A ; 
SHA1:                                S H A '1' ; 
SHA2:                                S H A '2' ; 
SIGN:                                S I G N ; 
SIN:                                 S I N ; 
SLEEP:                               S L E E P ; 
SOUNDEX:                             S O U N D E X ; 
SQL_THREAD_WAIT_AFTER_GTIDS:         S Q L '_' T H R E A D '_' W A I T '_' A F T E R '_' G T I D S ;
SQRT:                                S Q R T ; 
SRID:                                S R I D ; 
STARTPOINT:                          S T A R T P O I N T ; 
STRCMP:                              S T R C M P ; 
STR_TO_DATE:                         S T R '_' T O '_' D A T E ; 
ST_AREA:                             S T '_' A R E A ; 
ST_ASBINARY:                         S T '_' A S B I N A R Y ; 
ST_ASTEXT:                           S T '_' A S T E X T ; 
ST_ASWKB:                            S T '_' A S W K B ; 
ST_ASWKT:                            S T '_' A S W K T ; 
ST_BUFFER:                           S T '_' B U F F E R ; 
ST_CENTROID:                         S T '_' C E N T R O I D ; 
ST_CONTAINS:                         S T '_' C O N T A I N S ; 
ST_CROSSES:                          S T '_' C R O S S E S ; 
ST_DIFFERENCE:                       S T '_' D I F F E R E N C E ; 
ST_DIMENSION:                        S T '_' D I M E N S I O N ; 
ST_DISJOINT:                         S T '_' D I S J O I N T ; 
ST_DISTANCE:                         S T '_' D I S T A N C E ; 
ST_ENDPOINT:                         S T '_' E N D P O I N T ; 
ST_ENVELOPE:                         S T '_' E N V E L O P E ; 
ST_EQUALS:                           S T '_' E Q U A L S ; 
ST_EXTERIORRING:                     S T '_' E X T E R I O R R I N G ; 
ST_GEOMCOLLFROMTEXT:                 S T '_' G E O M C O L L F R O M T E X T ; 
ST_GEOMCOLLFROMTXT:                  S T '_' G E O M C O L L F R O M T X T ; 
ST_GEOMCOLLFROMWKB:                  S T '_' G E O M C O L L F R O M W K B ; 
ST_GEOMETRYCOLLECTIONFROMTEXT:       S T '_' G E O M E T R Y C O L L E C T I O N F R O M T E X T ;
ST_GEOMETRYCOLLECTIONFROMWKB:        S T '_' G E O M E T R Y C O L L E C T I O N F R O M W K B ;
ST_GEOMETRYFROMTEXT:                 S T '_' G E O M E T R Y F R O M T E X T ; 
ST_GEOMETRYFROMWKB:                  S T '_' G E O M E T R Y F R O M W K B ; 
ST_GEOMETRYN:                        S T '_' G E O M E T R Y N ; 
ST_GEOMETRYTYPE:                     S T '_' G E O M E T R Y T Y P E ; 
ST_GEOMFROMTEXT:                     S T '_' G E O M F R O M T E X T ; 
ST_GEOMFROMWKB:                      S T '_' G E O M F R O M W K B ; 
ST_INTERIORRINGN:                    S T '_' I N T E R I O R R I N G N ; 
ST_INTERSECTION:                     S T '_' I N T E R S E C T I O N ; 
ST_INTERSECTS:                       S T '_' I N T E R S E C T S ; 
ST_ISCLOSED:                         S T '_' I S C L O S E D ; 
ST_ISEMPTY:                          S T '_' I S E M P T Y ; 
ST_ISSIMPLE:                         S T '_' I S S I M P L E ; 
ST_LINEFROMTEXT:                     S T '_' L I N E F R O M T E X T ; 
ST_LINEFROMWKB:                      S T '_' L I N E F R O M W K B ; 
ST_LINESTRINGFROMTEXT:               S T '_' L I N E S T R I N G F R O M T E X T ; 
ST_LINESTRINGFROMWKB:                S T '_' L I N E S T R I N G F R O M W K B ; 
ST_NUMGEOMETRIES:                    S T '_' N U M G E O M E T R I E S ; 
ST_NUMINTERIORRING:                  S T '_' N U M I N T E R I O R R I N G ; 
ST_NUMINTERIORRINGS:                 S T '_' N U M I N T E R I O R R I N G S ; 
ST_NUMPOINTS:                        S T '_' N U M P O I N T S ; 
ST_OVERLAPS:                         S T '_' O V E R L A P S ; 
ST_POINTFROMTEXT:                    S T '_' P O I N T F R O M T E X T ; 
ST_POINTFROMWKB:                     S T '_' P O I N T F R O M W K B ; 
ST_POINTN:                           S T '_' P O I N T N ; 
ST_POLYFROMTEXT:                     S T '_' P O L Y F R O M T E X T ; 
ST_POLYFROMWKB:                      S T '_' P O L Y F R O M W K B ; 
ST_POLYGONFROMTEXT:                  S T '_' P O L Y G O N F R O M T E X T ; 
ST_POLYGONFROMWKB:                   S T '_' P O L Y G O N F R O M W K B ; 
ST_SRID:                             S T '_' S R I D ; 
ST_STARTPOINT:                       S T '_' S T A R T P O I N T ; 
ST_SYMDIFFERENCE:                    S T '_' S Y M D I F F E R E N C E ; 
ST_TOUCHES:                          S T '_' T O U C H E S ; 
ST_UNION:                            S T '_' U N I O N ; 
ST_WITHIN:                           S T '_' W I T H I N ; 
ST_X:                                S T '_' X ; 
ST_Y:                                S T '_' Y ; 
SUBDATE:                             S U B D A T E ; 
SUBSTRING_INDEX:                     S U B S T R I N G '_' I N D E X ; 
SUBTIME:                             S U B T I M E ; 
SYSTEM_USER:                         S Y S T E M '_' U S E R ; 
TAN:                                 T A N ; 
TIMEDIFF:                            T I M E D I F F ; 
TIMESTAMPADD:                        T I M E S T A M P A D D ; 
TIMESTAMPDIFF:                       T I M E S T A M P D I F F ; 
TIME_FORMAT:                         T I M E '_' F O R M A T ; 
TIME_TO_SEC:                         T I M E '_' T O '_' S E C ; 
TOUCHES:                             T O U C H E S ; 
TO_BASE64:                           T O '_' B A S E '6' '4' ; 
TO_DAYS:                             T O '_' D A Y S ; 
TO_SECONDS:                          T O '_' S E C O N D S ; 
UCASE:                               U C A S E ; 
UNCOMPRESS:                          U N C O M P R E S S ; 
UNCOMPRESSED_LENGTH:                 U N C O M P R E S S E D '_' L E N G T H ; 
UNHEX:                               U N H E X ; 
UNIX_TIMESTAMP:                      U N I X '_' T I M E S T A M P ; 
UPDATEXML:                           U P D A T E X M L ; 
UPPER:                               U P P E R ; 
UUID:                                U U I D ; 
UUID_SHORT:                          U U I D '_' S H O R T ; 
VALIDATE_PASSWORD_STRENGTH:          V A L I D A T E '_' P A S S W O R D '_' S T R E N G T H ;
VERSION:                             V E R S I O N ; 
WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS:   W A I T '_' U N T I L '_' S Q L '_' T H R E A D '_' A F T E R '_' G T I D S ;
WEEKDAY:                             W E E K D A Y ; 
WEEKOFYEAR:                          W E E K O F Y E A R ; 
WEIGHT_STRING:                       W E I G H T '_' S T R I N G ; 
WITHIN:                              W I T H I N ; 
YEARWEEK:                            Y E A R W E E K ; 
Y_FUNCTION:                          Y ; 
X_FUNCTION:                          X ; 



// Operators
// Operators. Assigns

VAR_ASSIGN:                          ':=';
PLUS_ASSIGN:                         '+=';
MINUS_ASSIGN:                        '-=';
MULT_ASSIGN:                         '*=';
DIV_ASSIGN:                          '/=';
MOD_ASSIGN:                          '%=';
AND_ASSIGN:                          '&=';
XOR_ASSIGN:                          '^=';
OR_ASSIGN:                           '|=';


// Operators. Arithmetics

STAR:                                '*';
DIVIDE:                              '/';
MODULE:                              '%';
PLUS:                                '+';
MINUSMINUS:                          '--';
MINUS:                               '-';
DIV:                                 D I V ;
MOD:                                 M O D;


// Operators. Comparation

EQUAL_SYMBOL:                        '=';
GREATER_SYMBOL:                      '>';
LESS_SYMBOL:                         '<';
EXCLAMATION_SYMBOL:                  '!';


// Operators. Bit

BIT_NOT_OP:                          '~';
BIT_OR_OP:                           '|';
BIT_AND_OP:                          '&';
BIT_XOR_OP:                          '^';


// Constructors symbols

DOT:                                 '.';
LR_BRACKET:                          '(';
RR_BRACKET:                          ')';
COMMA:                               ',';
SEMI:                                ';';
AT_SIGN:                             '@';
ZERO_DECIMAL:                        '0';
ONE_DECIMAL:                         '1';
TWO_DECIMAL:                         '2';
SINGLE_QUOTE_SYMB:                   '\'';
DOUBLE_QUOTE_SYMB:                   '"';
REVERSE_QUOTE_SYMB:                  '`';
COLON_SYMB:                          ':';



// Charsets

CHARSET_REVERSE_QOUTE_STRING:        '`' CHARSET_NAME '`';



// File's sizes


FILESIZE_LITERAL:                    DEC_DIGIT+ ('K'|'M'|'G'|'T');



// Literal Primitives


START_NATIONAL_STRING_LITERAL:       'N' SQUOTA_STRING;
STRING_LITERAL:                      DQUOTA_STRING | SQUOTA_STRING;
DECIMAL_LITERAL:                     DEC_DIGIT+;
HEXADECIMAL_LITERAL:                 'X' '\'' (HEX_DIGIT HEX_DIGIT)+ '\''
                                     | '0X' HEX_DIGIT+;

REAL_LITERAL:                        (DEC_DIGIT+)? '.' DEC_DIGIT+
                                     | DEC_DIGIT+ '.' EXPONENT_NUM_PART
                                     | (DEC_DIGIT+)? '.' (DEC_DIGIT+ EXPONENT_NUM_PART)
                                     | DEC_DIGIT+ EXPONENT_NUM_PART;
NULL_SPEC_LITERAL:                   '\\' 'N';
BIT_STRING:                          BIT_STRING_L;
STRING_CHARSET_NAME:                 '_' CHARSET_NAME;




// Hack for dotID
// Prevent recognize string:         .123somelatin AS ((.123), FLOAT_LITERAL), ((somelatin), ID)
//  it must recoginze:               .123somelatin AS ((.), DOT), (123somelatin, ID)

DOT_ID:                              '.' ID_LITERAL;



// Identifiers

ID:                                  ID_LITERAL;
// DOUBLE_QUOTE_ID:                  '"' ~'"'+ '"';
REVERSE_QUOTE_ID:                    '`' ~'`'+ '`';
STRING_USER_NAME:                    (
                                       SQUOTA_STRING | DQUOTA_STRING 
                                       | BQUOTA_STRING | ID_LITERAL
                                     ) '@' 
                                     (
                                       SQUOTA_STRING | DQUOTA_STRING 
                                       | BQUOTA_STRING | ID_LITERAL
                                     );
LOCAL_ID:                            '@'
                                (
                                  [A-Z0-9._$]+ 
                                  | SQUOTA_STRING
                                  | DQUOTA_STRING
                                  | BQUOTA_STRING
                                );
GLOBAL_ID:                           '@' '@' 
                                (
                                  [A-Z0-9._$]+ 
                                  | BQUOTA_STRING
                                );


// Fragments for Literal primitives

fragment CHARSET_NAME:               ARMSCII8 | ASCII | BIG5 | BINARY | CP1250 
                                     | CP1251 | CP1256 | CP1257 | CP850 
                                     | CP852 | CP866 | CP932 | DEC8 | EUCJPMS 
                                     | EUCKR | GB2312 | GBK | GEOSTD8 | GREEK 
                                     | HEBREW | HP8 | KEYBCS2 | KOI8R | KOI8U 
                                     | LATIN1 | LATIN2 | LATIN5 | LATIN7 
                                     | MACCE | MACROMAN | SJIS | SWE7 | TIS620 
                                     | UCS2 | UJIS | UTF16 | UTF16LE | UTF32 
                                     | UTF8 | UTF8MB4;

fragment EXPONENT_NUM_PART:          'E' '-'? DEC_DIGIT+;
fragment ID_LITERAL:                 [A-Z_$0-9]*?[A-Z_$]+?[A-Z_$0-9]*;
fragment DQUOTA_STRING:              '"' ( '\\'. | '""' | ~('"'| '\\') )* '"';
fragment SQUOTA_STRING:              '\'' ('\\'. | '\'\'' | ~('\'' | '\\'))* '\'';
fragment BQUOTA_STRING:              '`' ( '\\'. | '``' | ~('`'|'\\'))* '`';
fragment HEX_DIGIT:                  [0-9A-F];
fragment DEC_DIGIT:                  [0-9];
fragment BIT_STRING_L:               'B' '\'' [01]+ '\'';



// Last tokens must generate Errors

ERROR_RECONGNIGION:                  .    -> channel(ERRORCHANNEL);

// case insensitive lexxer
fragment A : [aA]; // match either an 'a' or 'A'
fragment B : [bB];
fragment C : [cC];
fragment D : [dD];
fragment E : [eE];
fragment F : [fF];
fragment G : [gG];
fragment H : [hH];
fragment I : [iI];
fragment J : [jJ];
fragment K : [kK];
fragment L : [lL];
fragment M : [mM];
fragment N : [nN];
fragment O : [oO];
fragment P : [pP];
fragment Q : [qQ];
fragment R : [rR];
fragment S : [sS];
fragment T : [tT];
fragment U : [uU];
fragment V : [vV];
fragment W : [wW];
fragment X : [xX];
fragment Y : [yY];
fragment Z : [zZ];