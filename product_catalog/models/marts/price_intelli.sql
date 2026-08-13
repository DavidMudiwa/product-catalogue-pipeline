{{
    config(
        materialized = 'table',
    )
    
}}

select
    p.product_id,
    d.title,
    d.brand,
    d.category_slug,
    d.category_name,
    d.department_name,
    p.scraped_at,
    date(p.scraped_at) as scrape_date,
    p.price_min,
    p.price_max,
    p.listing_price,
    p.discount_pct,
    p.is_multi_offer
from {{ ref('fct_price_snapshots') }} p
inner join {{ ref('dim_products') }} d
    on p.product_id = d.product_id
    and d.is_current = true