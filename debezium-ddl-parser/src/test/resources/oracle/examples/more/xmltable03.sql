-- XMLTable and outer join methods: http://anononxml.blogspot.com/2011/06/xmltable-and-outer-join-methods.html

select master_id
          ,details_id
          ,sub_details_id
   from demo4 s
        ,XMLTable('master' 
                  passing (s.xml) 
                  columns master_id varchar2 (20) path 'id'
                          ,details XMLType path 'details/detail') mstr
        ,XMLTable('detail' 
                  passing (mstr.details) 
                  columns details_id varchar2 (20) path 'id'
                          ,sub_details XMLType path 'sub_details/sub_detail')(+) dtl
        ,XMLTable('sub_detail' 
                  passing (dtl.sub_details) 
                  columns sub_details_id varchar2 (20) path 'id')(+) sub_dtl;