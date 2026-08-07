{{config (
    materialized = 'incremental',
    incremental_strategy = 'merge',
)}}

with stg_products as (
    select * from {{ ref('stg_products') }}
)

select 
    product_id,
    in_stock,
    stock_status,
    is_preorder,
    scraped_at
from stg_products


{% if is_incremental() %}
    where scraped_at > (select max(scraped_at) from {{ this }})
{% endif %}