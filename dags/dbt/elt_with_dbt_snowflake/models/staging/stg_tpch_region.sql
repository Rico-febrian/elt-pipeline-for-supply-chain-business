select *
from {{ source('stg_warehouse_db', 'region') }}