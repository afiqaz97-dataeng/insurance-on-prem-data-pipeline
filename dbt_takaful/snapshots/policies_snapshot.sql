{#
    SCD Type 2 on status, agent_id, contribution_amount — plan.md Section 5.
    Sources from stg_policies (not the raw source directly): staging already
    dedupes to the latest raw-zone partition and casts types, both of which a
    snapshot needs — a duplicate unique_key across partitions would break
    `unique_key='policy_id'` outright.
#}
{% snapshot policies_snapshot %}
{{
    config(
      target_schema='snapshots',
      unique_key='policy_id',
      strategy='check',
      check_cols=['status', 'agent_id', 'contribution_amount'],
    )
}}
select * from {{ ref('stg_policies') }}
{% endsnapshot %}
