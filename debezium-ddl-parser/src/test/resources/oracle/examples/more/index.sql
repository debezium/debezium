create index IDX on OBJ (CODE, DOCUMENT);

ALTER INDEX supplier_idx
  RENAME TO supplier_index_name;

DROP INDEX index_name;


CREATE INDEX ord_customer_ix
   ON orders (customer_id);

CREATE INDEX ord_customer_ix_demo
   ON orders (customer_id, sales_rep_id)
   COMPRESS 1;

CREATE INDEX ord_customer_ix_demo
   ON orders (order_mode)
   NOSORT
   NOLOGGING;

CREATE INDEX idx_personnel ON CLUSTER personnel;

CREATE INDEX upper_ix ON employees (UPPER(last_name));

CREATE INDEX income_ix
   ON employees(salary + (salary*commission_pct));

CREATE INDEX src_idx ON print_media(text_length(ad_sourcetext));

CREATE INDEX area_idx ON rect_tab x (x.area());

CREATE INDEX cust_last_name_ix ON customers (cust_last_name)
  GLOBAL PARTITION BY HASH (cust_last_name)
  PARTITIONS 4;

CREATE INDEX prod_idx ON hash_products(category_id) LOCAL
   STORE IN (tbs_01, tbs_02);

CREATE BITMAP INDEX product_bm_ix
   ON hash_products(list_price)
   TABLESPACE tbs_04;

CREATE UNIQUE INDEX nested_tab_ix
      ON textdocs_nestedtab(NESTED_TABLE_ID, document_typ);

ALTER INDEX ord_customer_ix REBUILD REVERSE;

ALTER INDEX ord_customer_ix REBUILD PARALLEL;

ALTER INDEX oe.cust_lname_ix
    INITRANS 5;

ALTER INDEX upper_ix PARALLEL;

ALTER INDEX upper_ix RENAME TO upper_name_ix;

ALTER INDEX cost_ix
   MODIFY PARTITION p2 UNUSABLE;

ALTER INDEX cost_ix UNUSABLE;

ALTER INDEX cost_ix
   REBUILD PARTITION p2;

ALTER INDEX cost_ix
   REBUILD PARTITION p3 NOLOGGING;

ALTER INDEX cost_ix MODIFY PARTITION p3
   STORAGE(MAXEXTENTS 30) LOGGING;

ALTER INDEX cost_ix
   RENAME PARTITION p3 TO p3_Q3;

ALTER INDEX cost_ix
   SPLIT PARTITION p2 AT (1500)
   INTO ( PARTITION p2a TABLESPACE tbs_01 LOGGING,
          PARTITION p2b TABLESPACE tbs_02);

ALTER INDEX cost_ix
   DROP PARTITION p1;

ALTER INDEX prod_idx
      MODIFY DEFAULT ATTRIBUTES INITRANS 5;
