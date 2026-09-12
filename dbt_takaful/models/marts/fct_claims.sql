-- Joined to dim_policies on the effective date range so a claim filed while a
-- policy was Active still shows Active in historical reporting, even if the
-- policy later lapsed (the exact scenario plan.md Section 5 calls out).
select
    cl.claim_id,
    cl.policy_id,
    dp.policy_sk,
    cl.claim_type,
    cl.claim_date,
    cl.claim_amount,
    cl.fund_type,
    cl.status,
    cl.approval_date
from {{ ref('stg_claims') }} cl
left join {{ ref('dim_policies') }} dp
    on cl.policy_id = dp.policy_id
    and cl.claim_date >= dp.valid_from
    and cl.claim_date <  dp.valid_to
