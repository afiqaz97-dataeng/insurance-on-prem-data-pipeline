{#
    Audit/lineage columns for the marts — there was previously no way to tell
    when a row was produced. Two different timestamps answer two different
    questions, since `dbt run` (transform) and load.py (load into MSSQL) are
    separate steps that can run minutes apart:
      - dbt_run_started_at: which dbt invocation transformed this row (set here).
      - etl_loaded_at: when this row actually landed in CuratedTakafulPOC.marts
        (set in scripts/load.py, not here — dbt doesn't know when load.py runs).
#}
{% macro dbt_run_started_at_col() %}
cast('{{ run_started_at.strftime("%Y-%m-%d %H:%M:%S") }}' as timestamp) as dbt_run_started_at
{% endmacro %}
