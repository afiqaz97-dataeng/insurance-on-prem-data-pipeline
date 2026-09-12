select
    cast(policy_id as varchar)                  as policy_id,
    cast(participant_id as varchar)             as participant_id,
    cast(product_name as varchar)               as product_name,
    cast(product_category as varchar)           as product_category,
    cast(has_pif as boolean)                    as has_pif,
    cast(start_date as date)                    as start_date,
    cast(end_date as date)                      as end_date,
    cast(term_years as integer)                 as term_years,
    cast(payment_mode as varchar)               as payment_mode,
    cast(contribution_amount as decimal(12,2))  as contribution_amount,
    cast(status as varchar)                     as status,
    cast(agent_id as varchar)                   as agent_id,
    cast(branch as varchar)                     as branch
from {{ source('raw', 'policies') }}
{{ latest_partition_only() }}
