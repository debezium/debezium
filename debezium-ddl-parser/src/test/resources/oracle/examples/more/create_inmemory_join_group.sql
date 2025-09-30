CREATE INMEMORY JOIN GROUP prod_id1
  (inventories(product_id), order_items(product_id));
CREATE INMEMORY JOIN GROUP prod_id2
  (inventories(product_id), pm.online_media(product_id));
