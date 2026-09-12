select
    cast(participant_id as varchar)    as participant_id,
    cast(full_name as varchar)         as full_name,
    cast(ic_number as varchar)         as ic_number,
    cast(date_of_birth as date)        as date_of_birth,
    cast(gender as varchar)            as gender,
    cast(state as varchar)             as state,
    cast(join_date as date)            as join_date
from {{ source('raw', 'participants') }}
{{ latest_partition_only() }}
