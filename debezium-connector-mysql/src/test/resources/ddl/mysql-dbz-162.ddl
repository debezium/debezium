CREATE DEFINER=`trunk2`@`%` FUNCTION `fnContainsMeasure`(
 physId int,
 measureId int
 ) RETURNS tinyint(1)
    DETERMINISTIC
begin
 select
 s.Result
 from (
 SELECT exists(
 SELECT
 ph.PhysicianId is not null as Result
 FROM Physician ph
 JOIN ReportingOptions ro ON ph.ReportingOptionsId = ro.Id and ph.PhysicianId = physId
 JOIN ReportingOptionsIndividualMeasure roim ON ro.Id = roim.ReportingOptionsId
 WHERE
 roim.IndividualMeasureId = measureId
 ) as Result
 ) s
 limit 1
 into @result;
 return @result;
end;

CREATE TABLE `test` (id INT(11) UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT);

ALTER TABLE `test` CHANGE `id` `collection_id` INT(11)
UNSIGNED
NOT NULL
AUTO_INCREMENT;

