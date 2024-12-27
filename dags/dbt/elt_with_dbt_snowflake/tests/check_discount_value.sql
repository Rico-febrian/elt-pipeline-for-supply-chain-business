-- Singular test: Check for negative discount values in all fact tables

-- Check total_discount_value column
select
    'fct_sales_summary' as source_table,
    total_discount_value
from 
    {{ ref('fct_sales_summary') }}
where 
    total_discount_value < 0

union all

-- Check discount_price column
select
    'fct_order_lineitem' as source_table,
    discount_price
from 
    {{ ref('fct_order_lineitem') }}
where
    discount_price < 0