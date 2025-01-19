/* transformação principal para stg_person_address */

with address_raw as (
    -- fonte de dados bruta da tabela 'address'
    select * 
    from {{ source('RAW_AW_ADDRESS', 'ADDRESS')}}
),

address_transformation as (
    select

        addressid as address_id
        , initcap(addressline1) as addressline1
        , initcap(addressline2) as addressline2
        , initcap(city) as city
        , stateprovinceid as id_state_province
        , coalesce(postalcode, 'UNKNOWN') as postal_code
        , spatiallocation as spatial_location
        , rowguid as rowguide
        , modifieddate as last_updated_at

    from address_raw
)

select * 
from address_transformation
