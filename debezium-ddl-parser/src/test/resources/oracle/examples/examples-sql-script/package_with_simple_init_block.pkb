-- The package init block should not be parsed as a variable declaration
create package body MyPackage as
begin
  OtherPackage.DoSomething;
end MyPackage;
