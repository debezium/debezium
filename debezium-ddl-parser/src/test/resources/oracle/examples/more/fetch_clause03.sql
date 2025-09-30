select color, favorite
FROM green_table
order by color, favorite
offset 1 rows
fetch NEXT 20 ROW WITH TIES;
