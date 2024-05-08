IF EXISTS(select 1 from sys.tables where name = '#' AND is_tracked_by_cdc=1)
    BEGIN
        RETURN
    END
ELSE
    BEGIN
        DECLARE @wrapper_functions TABLE(
            function_name SYSNAME,
            create_script NVARCHAR(MAX))

        INSERT INTO @wrapper_functions
        EXEC sys.sp_cdc_generate_wrapper_function

        DECLARE @create_script NVARCHAR(MAX)

        DECLARE [#hfunctions] CURSOR LOCAL FAST_FORWARD
        FOR
            SELECT create_script
            FROM @wrapper_functions

        OPEN [#hfunctions]
        FETCH [#hfunctions]
        INTO @create_script
        WHILE (@@fetch_status <> -1)
            BEGIN
                EXEC sp_executesql @create_script
                FETCH [#hfunctions]
                INTO @create_script
            END

        CLOSE [#hfunctions]
        DEALLOCATE [#hfunctions]
    END
