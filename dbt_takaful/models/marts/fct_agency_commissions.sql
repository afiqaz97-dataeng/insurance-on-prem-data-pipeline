-- agent_id is a plain attribute here, not a dimension FK — there's no
-- dim_agents (see CLAUDE.md SCD section for why it was dropped in Phase 4).
-- commission_amount_shareholders_fund is paid from the Shareholders' Fund,
-- not PRF/PIF — kept separate, never merged with contribution fund columns.
select
    t.transaction_id,
    t.agent_id,
    t.policy_id,
    dp.policy_sk,
    t.transaction_type,
    t.transaction_date,
    t.commission_rate,
    t.commission_amount_shareholders_fund,
    {{ dbt_run_started_at_col() }}
from {{ ref('stg_agency_transactions') }} t
left join {{ ref('dim_policies') }} dp
    on t.policy_id = dp.policy_id
    and t.transaction_date >= dp.valid_from
    and t.transaction_date <  dp.valid_to
