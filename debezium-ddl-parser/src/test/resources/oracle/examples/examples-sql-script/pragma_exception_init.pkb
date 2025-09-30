create or replace package body pragma_exception_init
is

  some_exception exception;
  pragma exception_init (some_exception, -20001);

end pragma_exception_init;
/
