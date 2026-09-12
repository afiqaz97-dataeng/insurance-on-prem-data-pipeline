select
    cast(claim_id as varchar)           as claim_id,
    cast(policy_id as varchar)          as policy_id,
    cast(claim_type as varchar)         as claim_type,
    cast(claim_date as date)            as claim_date,
    cast(claim_amount as decimal(12,2)) as claim_amount,
    cast(fund_type as varchar)          as fund_type,
    cast(status as varchar)             as status,
    cast(approval_date as date)         as approval_date
from {{ source('raw', 'claims') }}
{{ latest_partition_only() }}
