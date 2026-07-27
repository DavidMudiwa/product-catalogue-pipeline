-- In prod, read the real raw table loaded by load_to_bigquery.py.
-- In dev, read the local seed instead, so you can iterate on models without
-- touching BigQuery at all. This is the one place in the whole project that
-- knows about this dev/prod split -- everything downstream just uses
-- ref("stg_products") and never needs to care.
{% if target.name == 'prod' %}
    with raw as (
        select * from {{ source('prod_ecom', 'products') }}
    )
{% else %}
    with raw as (
        select * from {{ source('ecom_products', 'raw_products') }}
    )
{% endif %}

select
    cast(product_id as bigint)      as product_id,
    cast(tsin as bigint)             as tsin,
    cast(offer_id as bigint)         as offer_id,
    title,
    brand,
    slug,
    product_url,
    department_slug,
    category_slug,
    department_name,
    category_name,
    cast(price_min as {{ dbt.type_numeric()}})        as price_min,
    cast(price_max as {{ dbt.type_numeric()}})        as price_max,
    cast(listing_price as {{ dbt.type_numeric()}})    as listing_price,
    pretty_price,
    cast(discount_pct as {{ dbt.type_numeric()}})     as discount_pct,
    cast(is_multi_offer as boolean)  as is_multi_offer,
    cast(in_stock as boolean)        as in_stock,
    stock_status,
    cast(is_preorder as boolean)     as is_preorder,
    cast(rating as {{ dbt.type_numeric()}})           as rating,
    cast(reviews as integer)         as reviews,
    cast(rating_1_star as integer)   as rating_1_star,
    cast(rating_2_star as integer)   as rating_2_star,
    cast(rating_3_star as integer)   as rating_3_star,
    cast(rating_4_star as integer)   as rating_4_star,
    cast(rating_5_star as integer)   as rating_5_star,
    cast(scraped_at as timestamp)    as scraped_at,
    _source_file,
    cast(_loaded_at as timestamp)    as _loaded_at
from raw