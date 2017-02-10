CREATE DEFINER=`trunk2`@`%` TRIGGER PipelineHistory_insert AFTER INSERT ON Pipeline
 FOR EACH ROW BEGIN
 INSERT INTO PipelineHistory (
 PipelineId
 , InterfaceId
 , SourceHost
 , SourceDir
 , SourcePattern
 , SinkHost
 , SinkDir
 , SinkCommand
 , Active
 , Retries
 ) VALUES (
 NEW.PipelineId
 , NEW.InterfaceId
 , NEW.SourceHost
 , NEW.SourceDir
 , NEW.SourcePattern
 , NEW.SinkHost
 , NEW.SinkDir
 , NEW.SinkCommand
 , NEW.Active
 , NEW.Retries
 );
END