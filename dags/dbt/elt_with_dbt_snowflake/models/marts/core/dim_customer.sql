select
    -- Generate surrogate for customer dimension table 
    -- based on 'c_custkey' column in customer table from staging schema
    {{ dbt_utils.generate_surrogate_key( ['c_custkey'] ) }} as sk_customer_key,

    -- Add customer details from customer, nation and region table in staging schema
    customer.c_custkey as nk_customer_key,
    customer.c_name as cust_name,
    customer.c_address as cust_address,
    nation.n_name as cust_country,
    region.r_name as region,
    customer.c_phone as cust_phone,
    customer.c_acctbal as account_balance,
    customer.c_mktsegment as market_segment,

    -- Add timestamps for load tracking (e.g., when the record was created/updated)
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at

from {{ ref('stg_tpch_customer') }} as customer

-- Join with nation table to get customer country details 
join {{ ref('stg_tpch_nation') }} as nation 
    on customer.c_nationkey = nation.n_nationkey

-- Join with region table to get customer region details
join {{ ref('stg_tpch_region') }} as region
    on nation.n_regionkey = region.r_regionkey
