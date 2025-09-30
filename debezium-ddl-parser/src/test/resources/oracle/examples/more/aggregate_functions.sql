select collect(emp.ldap_login order by emp.last_name, emp.first_name)
  from employee emp
 where emp.dept = 'HR';
