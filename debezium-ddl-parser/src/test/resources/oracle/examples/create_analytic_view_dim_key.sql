CREATE OR REPLACE ANALYTIC VIEW sales_av
USING sales_fact
DIMENSION BY
  (time_attr_dim
    KEY (month_id) REFERENCES DISTINCT (year_id, quarter_id, month_id)
    HIERARCHIES (time_hier DEFAULT),
   geography_attr_dim
    KEY (geography_id) REFERENCES DISTINCT (country_id, region_id)
    HIERARCHIES (geography_hier DEFAULT))
MEASURES (sales FACT sales)
DEFAULT MEASURE sales;
