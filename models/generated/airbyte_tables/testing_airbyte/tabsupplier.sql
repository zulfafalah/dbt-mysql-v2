{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "testing_airbyte",
    post_hook = ["
                    {%
                        set scd_table_relation = adapter.get_relation(
                            database=this.database,
                            schema=this.schema,
                            identifier='tabsupplier_scd'
                        )
                    %}
                    {%
                        if scd_table_relation is not none
                    %}
                    {%
                            do adapter.drop_relation(scd_table_relation)
                    %}
                    {% endif %}
                        "],
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('tabsupplier_ab3') }}
select
    supplier_code,
    supplier_name,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_tabsupplier_hashid
from {{ ref('tabsupplier_ab3') }}
-- tabsupplier from {{ source('testing_airbyte', '_airbyte_raw_tabsupplier') }}
where 1 = 1
