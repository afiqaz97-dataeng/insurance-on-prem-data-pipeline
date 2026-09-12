{#
    Custom SCD2 tests — see plan.md Section 5: "a common SCD2 bug that silently
    corrupts historical fact joins without raising an obvious error." Applied to
    dim_policies/dim_participants in models/marts/_marts.yml.
#}

{% test no_scd_overlap(model, natural_key) %}
-- Any two DIFFERENT versions of the same natural key whose [valid_from, valid_to)
-- ranges overlap. Should always return 0 rows.
select
    a.{{ natural_key }},
    a.valid_from as a_valid_from, a.valid_to as a_valid_to,
    b.valid_from as b_valid_from, b.valid_to as b_valid_to
from {{ model }} a
join {{ model }} b
    on a.{{ natural_key }} = b.{{ natural_key }}
    and a.valid_from < b.valid_from
    and a.valid_from < b.valid_to
    and a.valid_to   > b.valid_from
{% endtest %}

{% test exactly_one_current_version(model, natural_key) %}
-- Every natural key must have exactly one is_current row. Should always return
-- 0 rows (0 = missing a current version, >1 = duplicate current versions).
select {{ natural_key }}, count(*) as current_version_count
from {{ model }}
where is_current
group by {{ natural_key }}
having count(*) != 1
{% endtest %}
