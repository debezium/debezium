-- ONE DDL TEST CASE FOR PROCEDURE WITH CURSOR
CREATE DEFINER=`parasshah`@`%` PROCEDURE `SP_GETCHILDBYID`(GIVEN_ID INT,GIVEN_EXAM_ID INT)
BEGIN
  DECLARE done INT DEFAULT 0;
  DECLARE p_child_name VARCHAR(2000);
  DECLARE P_MARKS INT;
  DECLARE bes INT;
  DECLARE A INT;
  DECLARE B VARCHAR(2000);
  DECLARE mark_status  int;
  DECLARE scale_test boolean;
  DECLARE scale_child_same int;
  DECLARE mark_status_name VARCHAR(2000);
  DECLARE marks_ab_dc varchar(2000);
  
 
  
  DECLARE cur1 CURSOR FOR 
  SELECT t_one.exam_id,t_one.marks
  FROM nodes_1 AS t_one WHERE t_one.parent_exam_id=GIVEN_ID ;
  
  DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET done = 1;
  
  
  select MS.NAME INTO marks_ab_dc  from nodes_1 s inner join EXAM E on(s.exam_id=E.EXAM_ID) INNER JOIN EXAM_MARKS EM ON(E.EXAM_ID=EM.EXAM_ID) 
  INNER JOIN NextExam.MARK_STATUS MS ON(EM.MARK_STATUS=MS.MARK_STATUS_ID);
  
 
 
  SELECT p1.scale_to into scale_test FROM nodes_1 p
  inner JOIN nodes_1 p1 on p1.parent_exam_id =p.parent_exam_id and p1.scale_to != p.scale_to and p1.parent_exam_id=GIVEN_ID group by p1.exam_id;
    
    
OPEN cur1;
REPEAT
FETCH cur1 INTO p_child_name,P_MARKS;

IF GIVEN_ID is null then
  select null;
END IF;

   
        UPDATE nodes_1 as t1 inner join (select sum(marks) as marks ,parent_exam_id  from nodes_1 group by parent_exam_id) t2 on(t1.exam_id=t2.parent_exam_id)
         SET t1.marks=t2.marks;
	 
	UPDATE nodes_1 as t1 inner join (select sum(pass_marks) as pass_marks,parent_exam_id from nodes_1 group by parent_exam_id) t2 on (t1.exam_id = t2.parent_exam_id) 
	set t1.pass_marks = t2.pass_marks;
	
	UPDATE nodes_1 as t1 inner join (select sum(max_marks) as max_marks,parent_exam_id from nodes_1 group by parent_exam_id) t2 on (t1.exam_id= t2.parent_exam_id) 
	set t1.max_marks = t2.max_marks;
	
	
	
      
IF NOT done THEN
		
 IF marks_ab_dc=1 THEN 
       UPDATE nodes_1 set marks=0 where exam_id=given_exam_id;
 END IF;
 
 IF marks_ab_dc=2 THEN
       UPDATE nodes_1 set marks=0 and max_marks=0 where exam_id=given_exam_id;
 END IF;


 

       SELECT EM.MARK_STATUS,MS.NAME  INTO mark_status,mark_status_name
	FROM EXAM E INNER JOIN EXAM_MARKS EM ON(E.EXAM_ID=EM.EXAM_ID) INNER JOIN NextExam.MARK_STATUS MS ON(EM.MARK_STATUS=MS.MARK_STATUS_ID) WHERE EM.EXAM_ID=GIVEN_EXAM_ID;

	SELECT MCR.MASTER_CALCULATION_RULE_ID,MCR.MASTER_CALCULATION_RULE_NAME INTO A,B
	FROM EXAM INNER JOIN EXAM_RULE ON (EXAM.EXAM_RULE_ID = EXAM_RULE.EXAM_RULE_ID) 
	INNER  JOIN MASTER_CALCULATION_RULE MCR ON (MCR.MASTER_CALCULATION_RULE_ID = EXAM_RULE.RULE_ID) AND (EXAM.EXAM_ID =GIVEN_EXAM_ID);

        UPDATE nodes_1 set master_calculation_rule_id=A,master_calculation_rule_name=B WHERE exam_id=GIVEN_EXAM_ID;
			
        SELECT B.BEST_EXAM_COUNT into bes FROM STUDENT S INNER JOIN EXAM  A ON(S.STD_ID=A.CLASS_ID) INNER JOIN EXAM_MARKS EM ON(EM.EXAM_ID=A.EXAM_ID)
        INNER JOIN EXAM_RULE B ON(A.EXAM_RULE_ID=B.EXAM_RULE_ID) WHERE A.EXAM_ID=GIVEN_EXAM_ID ORDER BY MARKS DESC LIMIT 1;
	
	update nodes_1 set best_exam_count=bes WHERE EXAM_ID=GIVEN_EXAM_ID;

        

	
	UPDATE nodes_1 as s join (select (marks/max_marks*scale_to) as scale_to_marks,exam_id from nodes_1 ) as p 
	               on(s.exam_id=p.exam_id) set s.scale_to_marks=p.scale_to_marks where marks <>0 and marks is not null and scale_to <>0 and max_marks is not null and pass_marks <>0 ;
			
	UPDATE nodes_1 as s join (select (case when scale_to=0 or scale_to is null then max_marks  else scale_to end) scaled_to_max_marks,exam_id from nodes_1)as p
		       on(s.exam_id=p.exam_id) set s.scaled_to_max_marks=p.scaled_to_max_marks;
		      
        UPDATE nodes_1 as s join (select  (pass_marks/max_marks * scale_to)  AS scaled_to_pass_marks,exam_id from nodes_1) as p 
                       on(s.exam_id=p.exam_id) set s.scaled_to_pass_marks=p.scaled_to_pass_marks;
		   
        UPDATE nodes_1 as s  join (select(scale_to*weightage/100) as weightaged_marks,exam_id from nodes_1 ) as p 
                       on(s.exam_id=p.exam_id) set s.weightaged_marks=p.weightaged_marks;
       
        UPDATE nodes_1 as s join( select (case when weightage=0 or weightage is null then max_marks else weightage end) weighted_max_marks,exam_id from nodes_1) as p
                       on(s.exam_id=p.exam_id) set s.weighted_max_marks=p.weighted_max_marks;
       
        UPDATE nodes_1 as s join (select  (pass_marks/max_marks * weightage)  AS weighted_pass_marks,exam_id from nodes_1) as p 
                       on(s.exam_id=p.exam_id) set s.weighted_pass_marks=p.weighted_pass_marks;
		       
 
 
 IF bes !=0 AND bes != null then      
      IF A=2 THEN        
        
	UPDATE nodes_1 as t1 inner join (select avg(marks) as marks ,parent_exam_id  from nodes_1  group by parent_exam_id ) t2 on(t1.exam_id=t2.parent_exam_id)
         SET t1.marks=t2.marks;
	 
	UPDATE nodes_1 as t1 inner join (select avg(pass_marks) as pass_marks,parent_exam_id from nodes_1 group by parent_exam_id) t2 on (t1.exam_id = t2.parent_exam_id) 
	set t1.pass_marks = t2.pass_marks;
	
	UPDATE nodes_1 as t1 inner join (select avg(max_marks) as max_marks,parent_exam_id from nodes_1 group by parent_exam_id) t2 on (t1.exam_id= t2.parent_exam_id) 
	set t1.max_marks = t2.max_marks;
	
	
	update nodes_1 as s inner join (select (case when scale_to=0 then avg(scaled_to_max_marks) else  scale_to  end) as scaled_to_max_marks,parent_exam_id from nodes_1 group by parent_exam_id) as p
                       on(s.exam_id=p.parent_exam_id) set s.scaled_to_max_marks=p.scaled_to_max_marks; 
		       
	 UPDATE nodes_1 as t1 inner join (select (case when scale_to=0 then avg(scale_to_marks) else (avg(scale_to_marks)/avg(scaled_to_max_marks)) * scale_to end) as scale_to_marks ,parent_exam_id  from nodes_1  group by parent_exam_id ) 
	as t2 on(t1.exam_id=t2.parent_exam_id) SET t1.scale_to_marks=t2.scale_to_marks;
	
        update nodes_1 as s inner join(select(CASE when scale_to=0 then avg(scaled_to_pass_marks) else(avg(scaled_to_pass_marks)/avg(scaled_to_max_marks)) *scale_to end) as scaled_to_pass_marks,parent_exam_id from nodes_1 group by parent_exam_id)as p
                       on(s.exam_id=p.parent_exam_id) set s.scaled_to_pass_marks=p.scaled_to_pass_marks;
		       
		       
		       
        update nodes_1 as s inner join (select (case when(weightage=0) then avg(weightaged_marks) else avg(weightaged_marks) * (weightage/100) end) as weightaged_marks,parent_exam_id from nodes_1 group by parent_exam_id) as p
                     on(s.exam_id=p.parent_exam_id) set s.weightaged_marks=p.weightaged_marks;
		     
	update nodes_1 as s inner join(select (case when (weightage=0) then avg(weighted_max_marks) else avg(weighted_max_marks) * (weightage/100) end) as weighted_max_marks,parent_exam_id from nodes_1 group by parent_exam_id) as p
			on(s.exam_id=p.parent_exam_id) set s.weighted_max_marks=p.weighted_max_marks;
	
	update nodes_1 as s inner join(select(case when weightage=0 then avg(weighted_pass_marks) else avg(weighted_pass_marks) *(weightage/100) end) as weighted_pass_marks,parent_exam_id from nodes_1 group by parent_exam_id)as p
			on(s.exam_id=p.parent_exam_id) set s.weighted_pass_marks=p.weighted_pass_marks;
    

	
    ELSE 
    
         UPDATE nodes_1 as t1 inner join (select sum(marks) as marks ,parent_exam_id  from nodes_1  group by parent_exam_id ) t2 on(t1.exam_id=t2.parent_exam_id)
         SET t1.marks=t2.marks;
	 
	UPDATE nodes_1 as t1 inner join (select sum(pass_marks) as pass_marks,parent_exam_id from nodes_1 group by parent_exam_id) t2 on (t1.exam_id = t2.parent_exam_id) 
	set t1.pass_marks = t2.pass_marks;
	
	UPDATE nodes_1 as t1 inner join (select sum(max_marks) as max_marks,parent_exam_id from nodes_1 group by parent_exam_id) t2 on (t1.exam_id= t2.parent_exam_id) 
	set t1.max_marks = t2.max_marks;
	

	update nodes_1 as s inner join (select (case when scale_to=0 then sum(scaled_to_max_marks) else  scale_to  end) as scaled_to_max_marks,parent_exam_id from nodes_1 group by parent_exam_id) as p
                       on(s.exam_id=p.parent_exam_id) set s.scaled_to_max_marks=p.scaled_to_max_marks; 
		       
	 UPDATE nodes_1 as t1 inner join (select (case when scale_to=0 then sum(scale_to_marks) else (sum(scale_to_marks)/sum(scaled_to_max_marks)) * scale_to end) as scale_to_marks ,parent_exam_id  from nodes_1  group by parent_exam_id ) 
	as t2 on(t1.exam_id=t2.parent_exam_id) SET t1.scale_to_marks=t2.scale_to_marks;
	
        update nodes_1 as s inner join(select(CASE when scale_to=0 then sum(scaled_to_pass_marks) else(sum(scaled_to_pass_marks)/sum(scaled_to_max_marks)) *scale_to end) as scaled_to_pass_marks,parent_exam_id from nodes_1 group by parent_exam_id)as p
                       on(s.exam_id=p.parent_exam_id) set s.scaled_to_pass_marks=p.scaled_to_pass_marks;
		       
		       
		       
        update nodes_1 as s inner join (select (case when(weightage=0) then sum(weightaged_marks) else sum(weightaged_marks) * (weightage/100) end) as weightaged_marks,parent_exam_id from nodes_1 group by parent_exam_id) as p
                     on(s.exam_id=p.parent_exam_id) set s.weightaged_marks=p.weightaged_marks;
		     
	update nodes_1 as s inner join(select (case when (weightage=0) then sum(weighted_max_marks) else sum(weighted_max_marks) * (weightage/100) end) as weighted_max_marks,parent_exam_id from nodes_1 group by parent_exam_id) as p
			on(s.exam_id=p.parent_exam_id) set s.weighted_max_marks=p.weighted_max_marks;
	
	update nodes_1 as s inner join(select(case when weightage=0 then sum(weighted_pass_marks) else sum(weighted_pass_marks) *(weightage/100) end) as weighted_pass_marks,parent_exam_id from nodes_1 group by parent_exam_id)as p
			on(s.exam_id=p.parent_exam_id) set s.weighted_pass_marks=p.weighted_pass_marks;
   END IF;
   
   
ELSE

  IF A=2 THEN
        
	UPDATE nodes_1 as t1 inner join (select avg(marks) as marks ,parent_exam_id  from nodes_1  group by parent_exam_id ) t2 on(t1.exam_id=t2.parent_exam_id)
         SET t1.marks=t2.marks;
	 
	UPDATE nodes_1 as t1 inner join (select avg(pass_marks) as pass_marks,parent_exam_id from nodes_1 group by parent_exam_id) t2 on (t1.exam_id = t2.parent_exam_id) 
	set t1.pass_marks = t2.pass_marks;
	
	UPDATE nodes_1 as t1 inner join (select avg(max_marks) as max_marks,parent_exam_id from nodes_1 group by parent_exam_id) t2 on (t1.exam_id= t2.parent_exam_id) 
	set t1.max_marks = t2.max_marks;

	update nodes_1 as s inner join (select (case when scale_to=0 then avg(scaled_to_max_marks) else  scale_to  end) as scaled_to_max_marks,parent_exam_id from nodes_1 group by parent_exam_id) as p
                       on(s.exam_id=p.parent_exam_id) set s.scaled_to_max_marks=p.scaled_to_max_marks; 
		       
	 UPDATE nodes_1 as t1 inner join (select (case when scale_to=0 then avg(scale_to_marks) else (avg(scale_to_marks)/avg(scaled_to_max_marks)) * scale_to end) as scale_to_marks ,parent_exam_id  from nodes_1  group by parent_exam_id ) 
	as t2 on(t1.exam_id=t2.parent_exam_id) SET t1.scale_to_marks=t2.scale_to_marks;
	
        update nodes_1 as s inner join(select(CASE when scale_to=0 then avg(scaled_to_pass_marks) else(avg(scaled_to_pass_marks)/avg(scaled_to_max_marks)) *scale_to end) as scaled_to_pass_marks,parent_exam_id from nodes_1 group by parent_exam_id)as p
                       on(s.exam_id=p.parent_exam_id) set s.scaled_to_pass_marks=p.scaled_to_pass_marks;
		       
		       
		       
        update nodes_1 as s inner join (select (case when(weightage=0) then avg(weightaged_marks) else avg(weightaged_marks) * (weightage/100) end) as weightaged_marks,parent_exam_id from nodes_1 group by parent_exam_id) as p
                     on(s.exam_id=p.parent_exam_id) set s.weightaged_marks=p.weightaged_marks;
		     
	update nodes_1 as s inner join(select (case when (weightage=0) then avg(weighted_max_marks) else avg(weighted_max_marks) * (weightage/100) end) as weighted_max_marks,parent_exam_id from nodes_1 group by parent_exam_id) as p
			on(s.exam_id=p.parent_exam_id) set s.weighted_max_marks=p.weighted_max_marks;
	
	update nodes_1 as s inner join(select(case when weightage=0 then avg(weighted_pass_marks) else avg(weighted_pass_marks) *(weightage/100) end) as weighted_pass_marks,parent_exam_id from nodes_1 group by parent_exam_id)as p
			on(s.exam_id=p.parent_exam_id) set s.weighted_pass_marks=p.weighted_pass_marks;
    

	
    ELSE 

        UPDATE nodes_1 as t1 inner join (select sum(marks) as marks ,parent_exam_id  from nodes_1  group by parent_exam_id ) t2 on(t1.exam_id=t2.parent_exam_id)
         SET t1.marks=t2.marks;
	 
	UPDATE nodes_1 as t1 inner join (select sum(pass_marks) as pass_marks,parent_exam_id from nodes_1 group by parent_exam_id) t2 on (t1.exam_id = t2.parent_exam_id) 
	set t1.pass_marks = t2.pass_marks;
	
	UPDATE nodes_1 as t1 inner join (select sum(max_marks) as max_marks,parent_exam_id from nodes_1 group by parent_exam_id) t2 on (t1.exam_id= t2.parent_exam_id) 
	set t1.max_marks = t2.max_marks;
	update nodes_1 as s inner join (select (case when scale_to=0 then sum(scaled_to_max_marks) else  scale_to  end) as scaled_to_max_marks,parent_exam_id from nodes_1 group by parent_exam_id) as p
                       on(s.exam_id=p.parent_exam_id) set s.scaled_to_max_marks=p.scaled_to_max_marks; 
		       
	 UPDATE nodes_1 as t1 inner join (select (case when scale_to=0 then sum(scale_to_marks) else (sum(scale_to_marks)/sum(scaled_to_max_marks)) * scale_to end) as scale_to_marks ,parent_exam_id  from nodes_1  group by parent_exam_id ) 
	as t2 on(t1.exam_id=t2.parent_exam_id) SET t1.scale_to_marks=t2.scale_to_marks;
	
        update nodes_1 as s inner join(select(CASE when scale_to=0 then sum(scaled_to_pass_marks) else(sum(scaled_to_pass_marks)/sum(scaled_to_max_marks)) *scale_to end) as scaled_to_pass_marks,parent_exam_id from nodes_1 group by parent_exam_id)as p
                       on(s.exam_id=p.parent_exam_id) set s.scaled_to_pass_marks=p.scaled_to_pass_marks;
		       
		       
		       
        update nodes_1 as s inner join (select (case when(weightage=0) then sum(weightaged_marks) else sum(weightaged_marks) * (weightage/100) end) as weightaged_marks,parent_exam_id from nodes_1 group by parent_exam_id) as p
                     on(s.exam_id=p.parent_exam_id) set s.weightaged_marks=p.weightaged_marks;
		     
	update nodes_1 as s inner join(select (case when (weightage=0) then sum(weighted_max_marks) else sum(weighted_max_marks) * (weightage/100) end) as weighted_max_marks,parent_exam_id from nodes_1 group by parent_exam_id) as p
			on(s.exam_id=p.parent_exam_id) set s.weighted_max_marks=p.weighted_max_marks;
	
	update nodes_1 as s inner join(select(case when weightage=0 then sum(weighted_pass_marks) else sum(weighted_pass_marks) *(weightage/100) end) as weighted_pass_marks,parent_exam_id from nodes_1 group by parent_exam_id)as p
			on(s.exam_id=p.parent_exam_id) set s.weighted_pass_marks=p.weighted_pass_marks;
     END IF;
     
   END IF;
   
       update nodes_1 s left join GRADE_RANGE_DETAILS grd  on  (s.branch_id  = grd.branch_id) and
       ((s.marks/s.max_marks)*100 between grd.LOWER_RANGE and grd.UPPER_RANGE) and s.marks>0
        set s.grade=grd.REMARKS;

        update nodes_1 as s join ( select (marks/max_marks*100) as percentage ,exam_id from nodes_1) as p on(s.exam_id=p.exam_id) set s.percentage=p.percentage;
	
        UPDATE nodes_1 as s join(SELECT exam_id,FIND_IN_SET( marks, (    
        SELECT GROUP_CONCAT( marks
        ORDER BY marks  ) 
        FROM nodes_1)
        ) AS `rank`
        FROM nodes_1 group by exam_id) as p on(s.parent_exam_id=p.exam_id)
        set s.`rank`=p.`rank` WHERE exam_id =GIVEN_EXAM_ID;
	
      SELECT t1.* FROM nodes_1 AS t1
      WHERE  EXISTS
      ( SELECT *
        FROM nodes_1 AS t2
        WHERE parent_exam_id is not null
      ) and t1. exam_id=GIVEN_ID;

END IF;

IF done THEN     
     SELECT t1.*
     FROM nodes_1 AS t1
     WHERE  EXISTS
      ( SELECT *
        FROM nodes_1 AS t2
        WHERE parent_exam_id is not null
      ) and t1.exam_id=GIVEN_ID;

END IF;

UNTIL done END REPEAT;

CLOSE cur1;
END 