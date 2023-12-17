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
    fax,
    idx,
    top,
    bank,
    city,
    {{ adapter.quote('name') }},
    npwp,
    name1,
    {{ adapter.quote('owner') }},
    margin,
    parent,
    top_dn,
    _assign,
    country,
    top_prn,
    creation,
    currency,
    discount,
    modified,
    phone_no,
    pic_name,
    zip_code,
    _comments,
    _liked_by,
    address_1,
    address_2,
    city_name,
    docstatus,
    incentive,
    is_active,
    npwp_date,
    _user_tags,
    parenttype,
    _ab_cdc_lsn,
    bank_issuer,
    modified_by,
    parentfield,
    amended_from,
    credit_limit,
    pic_position,
    sub_category,
    discount_type,
    naming_series,
    supplier_bank,
    supplier_code,
    supplier_name,
    address_npwp_1,
    address_npwp_2,
    bank_guarantee,
    warranty_amount,
    sub_category_name,
    supplier_top_days,
    warranty_currency,
    _ab_cdc_deleted_at,
    _ab_cdc_updated_at,
    supplier_dn_top_days,
    warranty_bank_issuer,
    supplier_issuing_bank,
    supplier_sub_category,
    supplier_bank_guarantee,
    supplier_sub_category_name,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_tabsupplier_hashid
from {{ ref('tabsupplier_ab3') }}
-- tabsupplier from {{ source('testing_airbyte', '_airbyte_raw_tabsupplier') }}
where 1 = 1
