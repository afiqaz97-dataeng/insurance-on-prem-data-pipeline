-- gross_amount = prf_amount + pif_amount + wakalah_fee_shareholders_fund must
-- hold for every contribution (CLAUDE.md "Fund-segregation logic"). Fails the
-- build if any row violates it beyond rounding tolerance.
select
    contribution_id,
    gross_amount,
    prf_amount,
    pif_amount,
    wakalah_fee_shareholders_fund,
    gross_amount - (prf_amount + pif_amount + wakalah_fee_shareholders_fund) as diff
from {{ ref('stg_contributions') }}
where abs(gross_amount - (prf_amount + pif_amount + wakalah_fee_shareholders_fund)) > 0.01
