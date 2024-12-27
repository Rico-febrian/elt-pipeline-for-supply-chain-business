with aggregation as (
    select
    lineitem.l_orderkey as dd_order_key,
    lineitem.l_partkey,
    lineitem.l_suppkey,
    lineitem.l_linenumber,
    lineitem.l_shipdate,
    lineitem.l_commitdate,
    lineitem.l_receiptdate,
    lineitem.l_returnflag,
    lineitem.l_shipmode,
    lineitem.l_extendedprice as extended_price,
    lineitem.l_quantity as order_qty,
    round(sum(lineitem.l_extendedprice * lineitem.l_discount), 2) as discount_price,
    round(sum(lineitem.l_extendedprice * (1 - lineitem.l_discount) * lineitem.l_tax), 2) as tax_price,
    round(sum((lineitem.l_extendedprice * (1 - lineitem.l_discount)) * (1 + lineitem.l_tax)), 2) as total_price
    from {{ ref('stg_tpch_lineitem') }} as lineitem 
    group by
        lineitem.l_orderkey,
        lineitem.l_partkey,
        lineitem.l_suppkey,
        lineitem.l_linenumber,
        lineitem.l_shipdate,
        lineitem.l_commitdate,
        lineitem.l_receiptdate,
        lineitem.l_returnflag,
        lineitem.l_shipmode,
        lineitem.l_extendedprice,
        lineitem.l_quantity
)

select
    -- Generate surrogate for product dimension table 
    -- based on 'ps_partkey' and 'ps_suppkey' columns in partsupp table from staging schema
    {{ dbt_utils.generate_surrogate_key( ['agg.dd_order_key', 'dimprod.sk_part_key', 'agg.l_linenumber'] ) }} as sk_order_lineitem,

    -- Add degenerate dimension key from orders table in staging schema
    agg.dd_order_key,

    -- Add surrogate key from dimension table
    dimprod.sk_part_key,
    dimprod.sk_supplier_key,
    
    -- Aggregation
    agg.extended_price,
    agg.order_qty,
    agg.discount_price,
    agg.tax_price,
    agg.total_price,

    -- Add date details
    orderdate.date_day as order_date,
    shipdate.date_day as ship_date,
    commitdate.date_day as commit_date,
    receiptdate.date_day as receipt_date,

    -- Add categorical details
    orders.o_orderstatus as order_status,
    agg.l_returnflag as return_flag,
    agg.l_shipmode as ship_mode,

    -- Add timestamps for load tracking (e.g., when the record was created/updated)
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at

from aggregation as agg

-- Join with orders table to get order date
join {{ ref('stg_tpch_orders') }} as orders
    on agg.dd_order_key = orders.o_orderkey

-- Join with product dimension table to get product details
join {{ ref('dim_product') }} as dimprod
    on agg.l_partkey = dimprod.nk_part_key

-- Join with date dimension table to get order date details
join {{ ref('dim_date') }} as orderdate
    on orders.o_orderdate = orderdate.date_day

-- Join with date dimension table to get shipping date details
join {{ ref('dim_date') }} as shipdate
    on agg.l_shipdate = shipdate.date_day

-- Join with date dimension table to get commit date details
join {{ ref('dim_date') }} as commitdate
    on agg.l_commitdate = commitdate.date_day

-- Join with date dimension table to get receipt date details
join {{ ref('dim_date') }} as receiptdate
    on agg.l_receiptdate = receiptdate.date_day
