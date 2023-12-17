{{ config(
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_testing_airbyte",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('tabsupplier_ab1') }}
select
    cast(fax as {{ dbt_utils.type_bigint() }}) as fax,
    cast(idx as {{ dbt_utils.type_bigint() }}) as idx,
    cast(top as {{ dbt_utils.type_bigint() }}) as top,
    cast(bank as {{ dbt_utils.type_string() }}(1024)) as bank,
    cast(city as {{ dbt_utils.type_string() }}(1024)) as city,
    cast({{ adapter.quote('name') }} as {{ dbt_utils.type_string() }}(1024)) as {{ adapter.quote('name') }},
    cast(npwp as {{ dbt_utils.type_string() }}(1024)) as npwp,
    cast(name1 as {{ dbt_utils.type_string() }}(1024)) as name1,
    cast({{ adapter.quote('owner') }} as {{ dbt_utils.type_string() }}(1024)) as {{ adapter.quote('owner') }},
    cast(margin as {{ dbt_utils.type_float() }}) as margin,
    cast(parent as {{ dbt_utils.type_string() }}(1024)) as parent,
    cast(top_dn as {{ dbt_utils.type_bigint() }}) as top_dn,
    cast(_assign as {{ dbt_utils.type_string() }}(1024)) as _assign,
    cast(country as {{ dbt_utils.type_string() }}(1024)) as country,
    cast(top_prn as {{ dbt_utils.type_bigint() }}) as top_prn,
        case when creation regexp '\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*' THEN STR_TO_DATE(SUBSTR(creation, 1, 19), '%Y-%m-%dT%H:%i:%S')
        else cast(if(creation = '', NULL, creation) as datetime)
        end as creation
        ,
    cast(currency as {{ dbt_utils.type_string() }}(1024)) as currency,
    cast(discount as {{ dbt_utils.type_float() }}) as discount,
        case when modified regexp '\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*' THEN STR_TO_DATE(SUBSTR(modified, 1, 19), '%Y-%m-%dT%H:%i:%S')
        else cast(if(modified = '', NULL, modified) as datetime)
        end as modified
        ,
    cast(phone_no as {{ dbt_utils.type_bigint() }}) as phone_no,
    cast(pic_name as {{ dbt_utils.type_string() }}(1024)) as pic_name,
    cast(zip_code as {{ dbt_utils.type_string() }}(1024)) as zip_code,
    cast(_comments as {{ dbt_utils.type_string() }}(1024)) as _comments,
    cast(_liked_by as {{ dbt_utils.type_string() }}(1024)) as _liked_by,
    cast(address_1 as {{ dbt_utils.type_string() }}(1024)) as address_1,
    cast(address_2 as {{ dbt_utils.type_string() }}(1024)) as address_2,
    cast(city_name as {{ dbt_utils.type_string() }}(1024)) as city_name,
    cast(docstatus as {{ dbt_utils.type_bigint() }}) as docstatus,
    cast(incentive as {{ dbt_utils.type_float() }}) as incentive,
    cast(is_active as {{ dbt_utils.type_bigint() }}) as is_active,
        case when npwp_date = '' then NULL
        else cast(npwp_date as date)
        end as npwp_date
        ,
    cast(_user_tags as {{ dbt_utils.type_string() }}(1024)) as _user_tags,
    cast(parenttype as {{ dbt_utils.type_string() }}(1024)) as parenttype,
    cast(_ab_cdc_lsn as {{ dbt_utils.type_float() }}) as _ab_cdc_lsn,
    cast(bank_issuer as {{ dbt_utils.type_string() }}(1024)) as bank_issuer,
    cast(modified_by as {{ dbt_utils.type_string() }}(1024)) as modified_by,
    cast(parentfield as {{ dbt_utils.type_string() }}(1024)) as parentfield,
    cast(amended_from as {{ dbt_utils.type_string() }}(1024)) as amended_from,
    cast(credit_limit as {{ dbt_utils.type_float() }}) as credit_limit,
    cast(pic_position as {{ dbt_utils.type_string() }}(1024)) as pic_position,
    cast(sub_category as {{ dbt_utils.type_string() }}(1024)) as sub_category,
    cast(discount_type as {{ dbt_utils.type_string() }}(1024)) as discount_type,
    cast(naming_series as {{ dbt_utils.type_string() }}(1024)) as naming_series,
    cast(supplier_bank as {{ dbt_utils.type_string() }}(1024)) as supplier_bank,
    cast(supplier_code as {{ dbt_utils.type_string() }}(1024)) as supplier_code,
    cast(supplier_name as {{ dbt_utils.type_string() }}(1024)) as supplier_name,
    cast(address_npwp_1 as {{ dbt_utils.type_string() }}(1024)) as address_npwp_1,
    cast(address_npwp_2 as {{ dbt_utils.type_string() }}(1024)) as address_npwp_2,
    cast(bank_guarantee as {{ dbt_utils.type_float() }}) as bank_guarantee,
    cast(warranty_amount as {{ dbt_utils.type_float() }}) as warranty_amount,
    cast(sub_category_name as {{ dbt_utils.type_string() }}(1024)) as sub_category_name,
    cast(supplier_top_days as {{ dbt_utils.type_bigint() }}) as supplier_top_days,
    cast(warranty_currency as {{ dbt_utils.type_string() }}(1024)) as warranty_currency,
    cast(_ab_cdc_deleted_at as {{ dbt_utils.type_string() }}(1024)) as _ab_cdc_deleted_at,
    cast(_ab_cdc_updated_at as {{ dbt_utils.type_string() }}(1024)) as _ab_cdc_updated_at,
    cast(supplier_dn_top_days as {{ dbt_utils.type_bigint() }}) as supplier_dn_top_days,
    cast(warranty_bank_issuer as {{ dbt_utils.type_string() }}(1024)) as warranty_bank_issuer,
    cast(supplier_issuing_bank as {{ dbt_utils.type_string() }}(1024)) as supplier_issuing_bank,
    cast(supplier_sub_category as {{ dbt_utils.type_string() }}(1024)) as supplier_sub_category,
    cast(supplier_bank_guarantee as {{ dbt_utils.type_float() }}) as supplier_bank_guarantee,
    cast(supplier_sub_category_name as {{ dbt_utils.type_string() }}(1024)) as supplier_sub_category_name,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('tabsupplier_ab1') }}
-- tabsupplier
where 1 = 1

