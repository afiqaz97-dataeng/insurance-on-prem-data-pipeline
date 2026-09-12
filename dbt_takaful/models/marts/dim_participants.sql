-- SCD Type 2 on `state` only; Type 1 (overwrite) on full_name/ic_number.
--
-- Note: dbt snapshot's `check` strategy only creates a new row when a check_cols
-- field changes — it does NOT update non-tracked columns on an existing row when
-- the source changes. So pulling full_name/ic_number straight from the snapshot
-- would silently freeze them at whatever value existed when that row's state last
-- changed, not the real Type-1 "always current" behavior plan.md calls for.
-- Fix: join back to stg_participants (always current) for full_name/ic_number,
-- and take state/history only from the snapshot.
-- Same beginning-of-time sentinel on the earliest version as dim_policies —
-- see the comment there for why.
select
    {{ dbt_utils.generate_surrogate_key(['ps.participant_id', 'ps.dbt_valid_from']) }} as participant_sk,
    ps.participant_id,
    cur.full_name,
    cur.ic_number,
    ps.state,
    cur.date_of_birth,
    cur.gender,
    cur.join_date,
    case
        when row_number() over (partition by ps.participant_id order by ps.dbt_valid_from) = 1
            then timestamp '1900-01-01'
        else ps.dbt_valid_from
    end                                                as valid_from,
    coalesce(ps.dbt_valid_to, timestamp '9999-12-31') as valid_to,
    (ps.dbt_valid_to is null)                         as is_current
from {{ ref('participants_snapshot') }} ps
left join {{ ref('stg_participants') }} cur
    on ps.participant_id = cur.participant_id
