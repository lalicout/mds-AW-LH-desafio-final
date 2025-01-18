with person_raw as (
    select * 
    from {{ source('AW_RAW__PERSON', 'Person') }}
),
/* XMLs demographics e additionalcontactinfo excluídos
*/
person_transformation as (
    select
        
        businessentityid as person_id,

        --padronizando person_type

        upper(persontype) as person_type,
        
        -- Validanado emailpromotion

        case 
            when emailpromotion in (0, 1, 2) then emailpromotion
            else null
        end as email_promotion
        ,first_name
        ,last_name
        ,middlename
        ,modifieddate
        ,namestyle
        ,rowguid
        ,initcap(suffix) as suffix,
        ,initcap(title) as title
    from person_raw
)

select * from person_transformation
