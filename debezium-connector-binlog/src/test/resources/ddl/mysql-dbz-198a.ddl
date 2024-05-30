CREATE DEFINER=`parasshah`@`%` PROCEDURE `find_par`(seed INT)
BEGIN
  DECLARE i int default 0;
   DROP TABLE IF EXISTS _result;
   CREATE TEMPORARY TABLE _result (node int primary key,id int,type varchar(20));
   INSERT INTO _result(node,id,type) VALUES(seed,i,'exam');
   DROP TABLE IF EXISTS _tmp;
  CREATE TEMPORARY TABLE _tmp (parent_node int,child_node int);
  REPEAT
    TRUNCATE TABLE _tmp;
    INSERT INTO _tmp SELECT EM.PARENT_EXAM_ID as parent_node, _result.node as child_node
           FROM _result JOIN EXAM_MAPPING EM ON EM.CHILD_EXAM_ID = _result.node;  
    INSERT IGNORE INTO _result(node,id,type) values ((SELECT parent_node FROM _tmp),i+1,'exam' );
    UNTIL ROW_COUNT() = 0
  END REPEAT;
  select max(id) from _result into i;
  labelXYZ: REPEAT
    TRUNCATE TABLE _tmp;
    INSERT INTO _tmp SELECT EM.EXAM_STRUCTURE_ID as parent_node, _result.node as child_node
           FROM _result JOIN EXAM_STRUCTURE_EXAM_MAPPING EM ON EM.EXAM_ID = _result.node and _result.id = i;  
    INSERT IGNORE INTO _result(node,id,type) values ((SELECT parent_node FROM _tmp),i+1,'exam_structure');
    UNTIL ROW_COUNT() = 0
  END REPEAT labelXYZ;
  select max(id) from _result into i;
  labelABC: REPEAT
    TRUNCATE TABLE _tmp;
    INSERT INTO _tmp SELECT EM.PARENT_EXAM_STRUCTURE_ID as parent_node, _result.node as child_node
           FROM _result JOIN EXAM_STRUCTURE_MAPPING EM ON EM.CHILD_EXAM_STRUCTURE_ID = _result.node and _result.id = i;  
    if ((SELECT EM.PARENT_EXAM_STRUCTURE_ID as parent_node, _result.node as child_node
         FROM _result JOIN EXAM_STRUCTURE_MAPPING EM ON EM.CHILD_EXAM_STRUCTURE_ID = _result.node and _result.id = i)!= NULL)
    then
      INSERT IGNORE INTO _result(node,id,type) values ((SELECT parent_node FROM _tmp),i+1,'exam_structure');
    end if;
    UNTIL ROW_COUNT() = 0
  END REPEAT;
  TRUNCATE TABLE _tmp;
  SELECT * FROM _result;
  DROP TABLE _tmp;
  
  nestedBegin: BEGIN
    DECLARE i int default 0;
    doubleNestedBegin: BEGIN
        DECLARE i int default 0;
    END doubleNestedBegin;
  END;
END
