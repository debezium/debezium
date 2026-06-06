CREATE OR REPLACE PROCEDURE PROCESS_QUEUE(FPN_DATENQUELLE_ID NUMBER DEFAULT NULL) IS
    BEGIN
        DELETE
        FROM TP_TICKET_RAW DEL_T
        WHERE EXISTS (
                      WITH BASIS AS (SELECT * FROM TP_TICKET_RAW TT)
                      SELECT NULL
                      FROM BASIS BA
                      WHERE EXISTS (SELECT NULL
                                   FROM BASIS BE
                                   WHERE BE.TABLE_NAME = BA.TABLE_NAME
                                     AND BE.TP_TICKET_ART_ID = BA.TP_TICKET_ART_ID
                                     AND BE.REF_ID = BA.REF_ID
                                     AND BA.CREATION_DATE > BE.CREATION_DATE)
                                     AND DEL_T.ID = BA.ID
                        );
        COMMIT;
 END PROCESS_QUEUE;