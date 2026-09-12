-- Joined to dim_policies on the effective date range, never policy_id alone
-- (see CLAUDE.md "Critical rule"). Fund-segregation columns pass through
-- untouched from staging.
select
    c.contribution_id,
    c.policy_id,
    dp.policy_sk,
    c.contribution_date,
    c.gross_amount,
    c.prf_amount,
    c.pif_amount,
    c.wakalah_fee_shareholders_fund,
    c.payment_method,
    {{ dbt_run_started_at_col() }}
from {{ ref('stg_contributions') }} c
left join {{ ref('dim_policies') }} dp
    on c.policy_id = dp.policy_id
    and c.contribution_date >= dp.valid_from
    and c.contribution_date <  dp.valid_to
