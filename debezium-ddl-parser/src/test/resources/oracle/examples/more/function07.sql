create or replace function budget(job varchar2) return varchar2 SQL_MACRO is
begin
  return q'{
     select deptno, sum(sal) budget 
     from emp 
     where job = budget.job
     group by deptno
  }';
end;
