ALTER INMEMORY JOIN GROUP prod_id1
  ADD(product_descriptions(product_id));

ALTER INMEMORY JOIN GROUP prod_id1
REMOVE(product_descriptions(product_id));
