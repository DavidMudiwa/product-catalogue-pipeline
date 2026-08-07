{{config(
    materialized='incremental',
    incremental_strategy='merge',
)}}

with stg_products as (
    select * from {{ ref('stg_products') }}
)

select
    product_id,
    scraped_at,
    price_min,
    price_max,
    listing_price,
    pretty_price,
    discount_pct,
    is_multi_offer
from stg_products

{% if is_incremental() %}
    where scraped_at > (select max(scraped_at) from {{ this }})
{% endif %}