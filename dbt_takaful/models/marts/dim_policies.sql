-- SCD Type 2, built on policies_snapshot. Fact tables must join on
-- [valid_from, valid_to) — never on policy_id alone. See CLAUDE.md
-- "Critical rule" under SCD.
--
-- The earliest known version of each policy gets valid_from pushed back to a
-- beginning-of-time sentinel instead of its literal dbt_valid_from (the moment
-- this pipeline first snapshotted it). Without this, a policy's real history
-- (years of contributions/claims before the pipeline's first run) would fall
-- before valid_from and silently fail to join to any dimension version.
select
    {{ dbt_utils.generate_surrogate_key(['policy_id', 'dbt_valid_from']) }} as policy_sk,
    policy_id,
    participant_id,
    product_name,
    product_category,
    has_pif,
    start_date,
    end_date,
    term_years,
    payment_mode,
    status,
    agent_id,
    contribution_amount,
    case
        when row_number() over (partition by policy_id order by dbt_valid_from) = 1
            then timestamp '1900-01-01'
        else dbt_valid_from
    end                                            as valid_from,
    coalesce(dbt_valid_to, timestamp '9999-12-31') as valid_to,
    (dbt_valid_to is null)                         as is_current
from {{ ref('policies_snapshot') }}
