{% macro extract_date_from_filename() %}
regexp_extract(filename, '/([0-9]{4}-[0-9]{2}-[0-9]{2})/[^/]+$', 1)
{% endmacro %}

{#
    Raw sources are read across every date partition ever written (see
    models/staging/_sources.yml) so the full raw history is preserved for audit.
    Staging models call this in a QUALIFY clause to keep only rows from the most
    recent partition — otherwise a second day's extract of unchanged mock data
    would silently duplicate every row.
#}
{% macro latest_partition_only() %}
qualify {{ extract_date_from_filename() }} = max({{ extract_date_from_filename() }}) over ()
{% endmacro %}
