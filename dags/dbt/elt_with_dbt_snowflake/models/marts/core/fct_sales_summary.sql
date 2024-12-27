with aggregation as (
select
    lineitem.l_orderkey,
    round(sum(lineitem.l_quantity)) as total_order_qty,
    round(sum(lineitem.l_extendedprice * lineitem.l_discount), 2) as total_discount_value,
    round(sum(lineitem.l_extendedprice * (1 - lineitem.l_discount) * lineitem.l_tax), 2) as total_tax_value,
    round(sum(partsupp.ps_supplycost * lineitem.l_quantity), 2) as cogs,
    orders.o_totalprice as revenue,
    round(orders.o_totalprice - sum(partsupp.ps_supplycost * lineitem.l_quantity), 2) as gross_profit,
    round((orders.o_totalprice - sum(partsupp.ps_supplycost * lineitem.l_quantity)) / orders.o_totalprice * 100, 2) AS profit_margin_percentage
from {{ ref('stg_tpch_lineitem') }} as lineitem 
join {{ ref('stg_tpch_orders') }} as orders
    on lineitem.l_orderkey = orders.o_orderkey
join {{ ref('stg_tpch_partsupp') }} as partsupp
    on lineitem.l_partkey = partsupp.ps_partkey
    and lineitem.l_suppkey = partsupp.ps_suppkey
group by 
    lineitem.l_orderkey,
    orders.o_totalprice
)
    
select
    -- Generate surrogate for product dimension table 
    -- based on 'ps_partkey' and 'ps_suppkey' columns in partsupp table from staging schema
    {{ dbt_utils.generate_surrogate_key( ['o_orderkey','o_custkey'] ) }} as sk_sales_summary,

    -- Add degenerate dimension key from orders table in staging schema
    orders.o_orderkey as dd_order_key,

    -- Add surrogate key from dimension table
    dimcust.sk_customer_key as sk_customer_key,

    -- Add order details
    orderdate.date_day as order_date,
    orders.o_orderstatus as order_status,

    -- Aggregation
    agg.total_order_qty,
    agg.total_discount_value,
    agg.total_tax_value,
    agg.revenue,
    agg.cogs,
    agg.gross_profit,
    agg.profit_margin_percentage,

    -- Add timestamps for load tracking (e.g., when the record was created/updated)
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at

from {{ ref('stg_tpch_orders') }} as orders

-- Join with aggregation cte table to get aggregation details 
join aggregation as agg
    on orders.o_orderkey = agg.l_orderkey 

-- Join with customer dimension table to get customer details
join {{ ref('dim_customer') }} as dimcust
    on orders.o_custkey = dimcust.nk_customer_key

-- Join with date dimension table to get order date details
join {{ ref('dim_date') }} as orderdate
    on orders.o_orderdate = orderdate.date_day
