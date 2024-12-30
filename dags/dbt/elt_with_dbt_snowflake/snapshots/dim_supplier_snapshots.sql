{% snapshot dim_supplier_snapshot %}

{{
    config(
        target_database="warehouse_db",
        target_schema="snapshots",
        unique_key="sk_supplier_key",

        strategy="check",
        check_cols=[
            'supp_name',
            'supp_address',
            'supp_phone',
            'supp_country',
            'region'
        ]
    )
}}

select *
from {{ ref("dim_supplier") }} 

{% endsnapshot %}