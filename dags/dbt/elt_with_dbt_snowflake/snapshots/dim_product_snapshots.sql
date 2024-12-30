{% snapshot dim_product_snapshot %}

{{
    config(
        target_database="warehouse_db",
        target_schema="snapshots",
        unique_key="sk_part_key",

        strategy="check",
        check_cols=[
            'part_name',
            'part_manufacturer',
            'part_brand',
            'part_type',
            'part_size',
            'part_container',
            'supply_cost',
            'retail_price'
        ]
    )
}}

select *
from {{ ref("dim_product") }} 

{% endsnapshot %}