{% snapshot dim_customer_snapshot %}

{{
    config(
        target_database="warehouse_db",
        target_schema="snapshots",
        unique_key="sk_customer_key",

        strategy="check",
        check_cols=[
            'cust_name',
            'cust_address',
            'cust_phone',
            'cust_country',
            'region',
            'market_segment'
        ]
    )
}}

select *
from {{ ref("dim_customer") }} 

{% endsnapshot %}