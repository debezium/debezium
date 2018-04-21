 -- ONE more DDL TEST CASE FOR FUNCTION
 CREATE DEFINER=`kiranb`@`%` FUNCTION `CHECK_FOR_BED_ALLOCATED`(BRANCHID BIGINT, STUDENTID BIGINT) RETURNS varchar(255) CHARSET utf8
    DETERMINISTIC
BEGIN
        DECLARE bedAllocated INT;
        IF STUDENTID > 0 THEN
                SET bedAllocated =(SELECT COUNT(*) FROM H_BED_ALLOCATION WHERE BRANCH_ID=BRANCHID AND STUDENT_ID=STUDENTID AND STATUS=200);
                IF bedAllocated >0 THEN
                     SELECT CONCAT (CONCAT(IFNULL(STD_FNAME,''), IFNULL(IF(STD_MNAME,'', CONCAT(' ', STD_MNAME)),''), IFNULL(IF(STD_LNAME,'', CONCAT(' ', STD_LNAME)),'')),' -',ADMISSION_NO) INTO @tempName FROM STUDENT WHERE BRANCH_ID=BRANCHID AND STD_ID=STUDENTID;
                     RETURN @tempName;
                ELSE
                     RETURN "";
                END IF;
        ELSE
                    RETURN "student id null";
        END IF;
    END
