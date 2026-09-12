-- commission_amount_shareholders_fund is paid from the Shareholders' Fund, not
-- PRF/PIF — passed through untouched, not merged with contribution fund columns.
select
    cast(transaction_id as varchar)                            as transaction_id,
    cast(agent_id as varchar)                                  as agent_id,
    cast(policy_id as varchar)                                 as policy_id,
    cast(transaction_type as varchar)                          as transaction_type,
    cast(transaction_date as date)                             as transaction_date,
    cast(commission_rate as decimal(5,2))                      as commission_rate,
    cast(commission_amount_shareholders_fund as decimal(12,2)) as commission_amount_shareholders_fund
from {{ source('raw', 'agency_transactions') }}
{{ latest_partition_only() }}
