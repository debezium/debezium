ALTER TABLE table_name
    MODIFY
        PARTITION BY RANGE (date_column)
            INTERVAL ( NUMTODSINTERVAL (1, 'DAY') )
            (
                PARTITION
                    VALUES LESS THAN
                        (TO_DATE('2024-01-01', 'YYYY-MM-DD'))
            );
