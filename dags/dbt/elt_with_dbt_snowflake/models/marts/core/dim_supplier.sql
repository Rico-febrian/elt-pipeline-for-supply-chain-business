select
    -- Generate surrogate for supplier dimension table 
    -- based on 's_suppkey' column in customer table from staging schema
    {{ dbt_utils.generate_surrogate_key( ['s_suppkey'] ) }} as sk_supplier_key,

    -- Add supplier details from supplier, nation and region table in staging schema
    supplier.s_suppkey as nk_supplier_key,
    supplier.s_name as supp_name,
    supplier.s_address as supp_address,
    nation.n_name as supp_country,
    region.r_name as region,
    supplier.s_phone as supp_phone,
    supplier.s_acctbal as account_balance,

    -- Add timestamps for load tracking (e.g., when the record was created/updated)
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at

from {{ ref('stg_tpch_supplier') }} as supplier

-- Join with nation table to get supplier country details 
join {{ ref('stg_tpch_nation') }} as nation 
    on supplier.s_nationkey = nation.n_nationkey

-- Join with region table to get supplier region details
join {{ ref('stg_tpch_region') }} as region
    on nation.n_regionkey = region.r_regionkey
