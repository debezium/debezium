CREATE OR REPLACE ANALYTIC VIEW sales_av
USING sales_fact
DIMENSION BY
  (time_attr_dim                         -- An attribute dimension of time data
    KEY month_id REFERENCES month_id
    HIERARCHIES (
      time_hier DEFAULT),
   product_attr_dim                      -- An attribute dimension of product data
    KEY category_id REFERENCES category_id
    HIERARCHIES (
      product_hier DEFAULT),
   geography_attr_dim                    -- An attribute dimension of store data
    KEY state_province_id
    REFERENCES state_province_id HIERARCHIES (
      geography_hier DEFAULT)
   )
MEASURES(
    sales FACT sales,                      -- A base measure
    units FACT units,                      -- A base measure
    -- Calculated measures
    sales_prior_period AS (LAG(sales) OVER (HIERARCHY time_hier OFFSET 1)),
    sales_year_ago AS (LAG(sales) OVER (HIERARCHY time_hier OFFSET 1 ACROSS ANCESTOR AT LEVEL year)),
    chg_sales_year_ago AS (LAG_DIFF(sales) OVER (HIERARCHY time_hier OFFSET 1 ACROSS ANCESTOR AT LEVEL year)),
    pct_chg_sales_year_ago AS (LAG_DIFF_PERCENT(sales) OVER (HIERARCHY time_hier OFFSET 1 ACROSS ANCESTOR AT LEVEL year)),
    sales_qtr_ago AS (LAG(sales) OVER (HIERARCHY time_hier OFFSET 1 ACROSS ANCESTOR AT LEVEL quarter)),
    chg_sales_qtr_ago AS (LAG_DIFF(sales) OVER (HIERARCHY time_hier OFFSET 1 ACROSS ANCESTOR AT LEVEL quarter)),
    pct_chg_sales_qtr_ago AS (LAG_DIFF_PERCENT(sales) OVER (HIERARCHY time_hier OFFSET 1 ACROSS ANCESTOR AT LEVEL quarter))
)
DEFAULT MEASURE SALES;

CREATE OR REPLACE ANALYTIC VIEW sales_av
USING av.sales_fact
DIMENSION BY
  (time_attr_dim
    KEY month_id REFERENCES month_id
    HIERARCHIES (
      time_hier DEFAULT),
   product_attr_dim
    KEY category_id REFERENCES category_id
    HIERARCHIES (
      product_hier DEFAULT),
   geography_attr_dim
    KEY state_province_id
    REFERENCES state_province_id
    HIERARCHIES (
      geography_hier DEFAULT)
   )
MEASURES
 (sales FACT sales,
  sales_year_ago AS (LAG(sales) OVER (HIERARCHY time_hier OFFSET 1 ACROSS ANCESTOR AT LEVEL year)),
  sales_pct_chg_year_ago AS (ROUND(LAG_DIFF_PERCENT(sales) OVER (HIERARCHY time_hier OFFSET 1 ACROSS ANCESTOR AT LEVEL year),2)),
  units FACT units
  )
DEFAULT MEASURE SALES;

CREATE OR REPLACE ANALYTIC VIEW sales_av
USING av.sales_fact
DIMENSION BY
  (time_attr_dim
    KEY month_id REFERENCES month_id
    HIERARCHIES (
      time_hier DEFAULT))
MEASURES
 (sales FACT sales,
  avg_sales FACT sales AGGREGATE BY AVG,
  count_sales FACT sales AGGREGATE BY COUNT,
  max_sales FACT sales AGGREGATE BY MAX,
  min_sales FACT sales AGGREGATE BY MIN,
  stddev_sales FACT sales AGGREGATE BY STDDEV,
  variance_sales FACT sales AGGREGATE BY VARIANCE,
  units FACT units,
  avg_units FACT units AGGREGATE BY AVG
  )
DEFAULT MEASURE SALES
DEFAULT AGGREGATE BY SUM;
