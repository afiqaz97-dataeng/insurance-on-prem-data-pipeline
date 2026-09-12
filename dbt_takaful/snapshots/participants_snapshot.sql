{#
    SCD Type 2 on `state` only — plan.md Section 5. full_name/ic_number are
    Type 1 (corrections, not history) and are intentionally NOT in check_cols;
    they're still selected below so dim_participants has the current value.
#}
{% snapshot participants_snapshot %}
{{
    config(
      target_schema='snapshots',
      unique_key='participant_id',
      strategy='check',
      check_cols=['state'],
    )
}}
select * from {{ ref('stg_participants') }}
{% endsnapshot %}
