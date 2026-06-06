SELECT
    XMLAGG(XMLELEMENT(X,first_name, ',')
        ORDER BY FIRST_NAME).extract('//text()').getClobVal() AS "NAMES"
FROM STUDENTS;

SELECT
    XMLCOLATTVAL(first_name,first_name, first_name).extract('//text()').getClobVal()
FROM STUDENTS;

SELECT
    XMLFOREST(first_name,first_name, first_name).extract('//text()').getClobVal()
FROM STUDENTS;

SELECT
    XMLELEMENT(TEST,first_name , ',').extract('//text()').getClobVal()
FROM STUDENTS;

SELECT XMLPARSE(CONTENT '124 <purchaseOrder poNo="12435">
   <customerName> Acme Enterprises</customerName>
   <itemNo>32987457</itemNo>
   </purchaseOrder>'
WELLFORMED).extract('//text()').getClobVal() AS PO FROM DUAL;

SELECT XMLPI(NAME "Order analysisComp", 'imported, reconfigured, disassembled').extract('//text()').getClobVal()
           AS "XMLPI" FROM DUAL;

SELECT warehouse_name,
       EXTRACTVALUE(warehouse_spec, '/Warehouse/Area'),
       XMLQuery(
               'for $i in /Warehouse
               where $i/Area > 50000
               return <Details>
                         <Docks num="{$i/Docks}"/>
                         <Rail>
                           {
                           if ($i/RailAccess = "Y") then "true" else "false"
                           }
                         </Rail>
                      </Details>' PASSING warehouse_spec RETURNING CONTENT).extract('//text()').getClobVal() "Big_warehouses"
FROM warehouses;

SELECT XMLROOT ( XMLType('<poid>143598</poid>'), VERSION '1.0', STANDALONE YES).extract('//text()').getClobVal()
           AS "XMLROOT" FROM DUAL;
