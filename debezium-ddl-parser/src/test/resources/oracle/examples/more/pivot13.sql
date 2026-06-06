WITH SessionCTE AS
         (
             SELECT
                 terminal
             FROM v$session
         )
SELECT
    *
FROM SessionCTE PIVOT
    (
        COUNT(*) FOR terminal IN ('VIRTUALORACLE11' AS oracle_local, 'DRUID' AS druid, 'THOR' AS thor)
    )