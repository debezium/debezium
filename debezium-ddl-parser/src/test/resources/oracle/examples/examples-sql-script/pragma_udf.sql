create package body MyPkg as 

function MyFunc 
  return varchar2 
is 

-- User Defined Function pragma
pragma udf; 

begin
  return ''; 
end MyFunc; 

end MyPkg;
