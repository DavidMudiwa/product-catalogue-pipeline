-- tests/assert_review_distribution_matches_total.sql
select product_id, scraped_at, reviews,
       (rating_1_star + rating_2_star + rating_3_star + rating_4_star + rating_5_star) as star_sum
from {{ ref('fct_review_snapshots') }}
where (rating_1_star + rating_2_star + rating_3_star + rating_4_star + rating_5_star) > reviews