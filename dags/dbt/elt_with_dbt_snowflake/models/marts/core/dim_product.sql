select
    -- Generate surrogate for product dimension table 
    -- based on 'ps_partkey' and 'ps_suppkey' columns in partsupp table from staging schema
    {{ dbt_utils.generate_surrogate_key( ['part.p_partkey', 'partsupp.ps_suppkey'] ) }} as sk_part_key,

    -- Add supplier surrogate key from supplier dimension table to get supplier details
    dimsupp.sk_supplier_key as sk_supplier_key,

    -- Add product details from part and partsupp in staging schema
    part.p_partkey as nk_part_key,
    part.p_name as part_name,
    part.p_mfgr as part_manufacturer,
    part.p_brand as part_brand,
    part.p_type as part_type,
    part.p_size as part_size,
    part.p_container as part_container,
    partsupp.ps_availqty as available_qty,
    part.p_retailprice as retail_price,
    partsupp.ps_supplycost as supply_cost,

    -- Add timestamps for load tracking (e.g., when the record was created/updated)
    {{ dbt_date.now() }} as created_at,
    {{ dbt_date.now() }} as updated_at

from {{ ref('stg_tpch_part') }} as part

-- Join with partsupp table to get part supplier details 
join {{ ref('stg_tpch_partsupp') }} as partsupp 
    on part.p_partkey = partsupp.ps_partkey

-- Join with supplier dimension table to get supplier details
join {{ ref('dim_supplier') }} as dimsupp
    on partsupp.ps_suppkey = dimsupp.nk_supplier_key
