CREATE JAVA CLASS USING BFILE (java_dir, 'Agent.class')
;
-- requires / as a stmt delimiter
-- CREATE JAVA SOURCE NAMED "Welcome" AS
--    public class Welcome {
--       public static String welcome() {
--          return "Welcome World";   } }
-- /
CREATE JAVA RESOURCE NAMED "appText"
   USING BFILE (java_dir, 'textBundle.dat')
;
