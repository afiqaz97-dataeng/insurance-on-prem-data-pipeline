-- Type 1, simple overwrite. No raw product catalog table exists — derived from
-- distinct (product_name, product_category) pairs on stg_policies. Verified
-- empirically that product_name maps 1:1 to product_category (7 stable
-- products, no conflicts), so this is a safe, static-in-practice dimension.
select
    {{ dbt_utils.generate_surrogate_key(['product_name']) }} as product_sk,
    product_name,
    product_category,
    {{ dbt_run_started_at_col() }}
from {{ ref('stg_policies') }}
group by product_name, product_category
