-- pif_amount must be 0 wherever the policy has no PIF component (CLAUDE.md
-- "Invariant to test everywhere this data passes through"). Tested at the mart
-- layer (fct_contributions + dim_policies) since that's what actually reaches
-- MSSQL/Power BI, not just staging.
select
    f.contribution_id,
    f.policy_id,
    f.pif_amount,
    dp.has_pif
from {{ ref('fct_contributions') }} f
join {{ ref('dim_policies') }} dp on f.policy_sk = dp.policy_sk
where dp.has_pif = false and f.pif_amount != 0
