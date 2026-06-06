DROP PACKAGE TCERS.PK_GRAMMAR_TEST;

CREATE OR REPLACE PACKAGE TCERS.PK_GRAMMAR_TEST AS 

  function test return number;

END PK_GRAMMAR_TEST;
/
DROP PACKAGE BODY TCERS.PK_GRAMMAR_TEST;

CREATE OR REPLACE PACKAGE BODY TCERS.PK_GRAMMAR_TEST AS

  function test return number AS
    src_row all_source% rowtype;
    n pls_integer;
    cursor src is select * from all_source;
  BEGIN
    open src;
    loop
      fetch src into src_row;
      
      if src% notfound then
        exit;
      end if;
      
      if src%  found then
        null;
      end if;
      
    end loop;
    
    if src%     isopen then
      close src;
    end if;
    
    RETURN sql%   rowcount;
  END test;

END PK_GRAMMAR_TEST;
/
