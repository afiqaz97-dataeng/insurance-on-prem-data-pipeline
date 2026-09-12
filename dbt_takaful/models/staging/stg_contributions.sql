-- Fund-segregation columns (gross_amount, prf_amount, pif_amount,
-- wakalah_fee_shareholders_fund) are passed through untouched — no splitting,
-- collapsing, or renaming. See CLAUDE.md "Fund-segregation logic".
select
    cast(contribution_id as varchar)                     as contribution_id,
    cast(policy_id as varchar)                            as policy_id,
    cast(contribution_date as date)                       as contribution_date,
    cast(gross_amount as decimal(12,2))                   as gross_amount,
    cast(wakalah_fee_shareholders_fund as decimal(12,2))  as wakalah_fee_shareholders_fund,
    cast(prf_amount as decimal(12,2))                     as prf_amount,
    cast(pif_amount as decimal(12,2))                     as pif_amount,
    cast(payment_method as varchar)                       as payment_method
from {{ source('raw', 'contributions') }}
{{ latest_partition_only() }}
