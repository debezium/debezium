IF EXISTS(select 1 from #db.sys.tables where name = ? AND is_tracked_by_cdc=1)
    BEGIN
        RETURN
    END
ELSE
    BEGIN
        DECLARE @wrapper_functions TABLE(
            function_name SYSNAME,
            create_script NVARCHAR(MAX))

        INSERT INTO @wrapper_functions
        EXEC #db.sys.sp_cdc_generate_wrapper_function

        DECLARE @create_script NVARCHAR(MAX)

        DECLARE # CURSOR LOCAL FAST_FORWARD
        FOR
            SELECT create_script
            FROM @wrapper_functions

        OPEN #
        FETCH #
        INTO @create_script
        WHILE (@@fetch_status <> -1)
            BEGIN
                EXEC sp_executesql @create_script
                FETCH #
                INTO @create_script
            END

        CLOSE #
        DEALLOCATE #
    END
