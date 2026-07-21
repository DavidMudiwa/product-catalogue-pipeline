{{config(
    materialized='table',
)}}

-- SCD Type 2 derived directly from stg_products' existing full history
-- (one row per product per scrape) -- not a dbt snapshot, since we already
-- have every historical observation rather than needing dbt to infer change
-- points by diffing against yesterday's state.

with stg_products as (
    select * from {{ ref('stg_products') }}
),

-- 1. Hash only the products we actually want to version on.
hashed as (
    select
        *,
        
    {{ dbt_utils.generate_surrogate_key([
    'title',
    'brand',
    'slug',
    'product_url',
    'department_slug',
    'category_slug',
    'department_name',
    'category_name']) }} as product_hash,
    from stg_products
),

-- 2. Flag every row where the tracked attributes differ from this same
--    product's previous scrape (or where it's the first scrape ever seen).
change_flagged as (
    select
        *,
        case
            when lag(product_hash) over (
                partition by product_id order by scraped_at
            ) is distinct from product_hash
            then 1 else 0
        end as is_change
    from hashed

),

-- 3. Cumulative sum of the change flag = a version number that stays flat
--    across consecutive unchanged scrapes and increments on real changes
--    (the "gaps and islands" pattern).
versioned as (
    select
        *,
        sum(is_change) over (
            partition by product_id order by scraped_at
            rows between unbounded preceding and current row -- This means:Include every row from the first scrape of this product up to the current row.
        ) as version_number
    from change_flagged
),

-- 4. Collapse each (product_id, version_number) island into a single row.
collapsed as (
    select
        product_id,
        version_number,
        min(tsin) as tsin,                     
        min(offer_id) as offer_id,              
        min(title) as title,
        min(brand) as brand,
        min(slug) as slug,
        min(product_url) as product_url,
        min(department_slug) as department_slug,
        min(category_slug) as category_slug,
        min(department_name) as department_name,
        min(category_name) as category_name,
        min(scraped_at) as valid_from
    from versioned
    group by product_id, version_number
),

-- 5. valid_to = the next version's valid_from for the same product.
--    NULL naturally falls out for the current/latest version.
final as (
    select
        *,
        lead(valid_from) over (
            partition by product_id order by version_number
        ) as valid_to
    from collapsed
)

select
    {{ dbt_utils.generate_surrogate_key(['product_id', 'valid_from']) }} as product_key,
    product_id,
    tsin,
    offer_id,
    title,
    brand,
    slug,
    product_url,
    department_slug,
    category_slug,
    department_name,
    category_name,
    valid_from,
    valid_to,
    valid_to is null as is_current
from final