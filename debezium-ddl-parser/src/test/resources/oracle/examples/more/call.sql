CALL my_procedure(arg1 => 3, arg2 => 4) ;

CALL my_procedure(3, 4) ;

CALL my_procedure(3, arg2 => 4) ;

CALL emp_mgmt.remove_dept(162);

CALL warehouse_typ(456, 'Warehouse 456', 2236).ret_name() INTO :x;