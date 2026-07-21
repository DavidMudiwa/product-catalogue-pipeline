-- Fails if any product has zero or more than one is_current = true row.
select product_id, count(*) as current_row_count
from {{ ref('dim_products') }}
where is_current
group by product_id
having count(*) != 1