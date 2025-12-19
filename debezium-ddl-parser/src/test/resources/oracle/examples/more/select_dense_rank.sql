-- undocumented (?) feature of first/last: you could replace mandatory "order by" clause by "partition by" clause
select max(dummy) keep(dense_rank first partition by dummy)
     , max(dummy) keep(dense_rank last partition by dummy)
from dual;
