
VALUES ASNCDC.ASNCDCSERVICES('status','asncdc');

CALL ASNCDC.ADDTABLE('db2inst1', 'products' ); 
CALL ASNCDC.ADDTABLE('db2inst1', 'products_on_hand' ); 
CALL ASNCDC.ADDTABLE('db2inst1', 'customers' );
CALL ASNCDC.ADDTABLE('db2inst1', 'orders' ); 

VALUES ASNCDC.ASNCDCSERVICES('reinit','asncdc');