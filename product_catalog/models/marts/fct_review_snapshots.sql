{{config(
    materialized='incremental',
    incremental_strategy='merge',
)}}

with stg_products as (
    select * from {{ ref('stg_products') }}
)

select 
    product_id,
    rating,
    reviews,
    rating_1_star,
    rating_2_star,
    rating_3_star,
    rating_4_star,
    rating_5_star,
    scraped_at
from stg_products

{% if is_incremental() %}
    where scraped_at > (select max(scraped_at) from {{ this }})
{% endif %}