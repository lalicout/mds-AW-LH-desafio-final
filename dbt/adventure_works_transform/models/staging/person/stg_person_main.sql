/* Transformação principal */

with person_raw as (
    select * 
    from {{ source('RAW_AW_PERSON', 'Person') }}
),

person_transformation as (
    select
        
        businessentityid as person_id

        --padronizando person_type

        ,upper(persontype) as person_type
        
        -- Validanado emailpromotion

        ,case 
            when emailpromotion in (0, 1, 2) then emailpromotion
            else null
        end as email_promotion
        ,concat_ws(' ', firstname, middlename, lastname) as full_name
        ,middlename as middle_name
        ,modifieddate
        ,namestyle
        ,rowguid
        ,initcap(suffix) as suffix
        ,initcap(title) as title
        
        /* Extraíndo dados do XML
        */

        ,xmlget(demographics, 'TotalPurchaseYTD'):"$" as total_purchase_ytd      
        ,xmlget(demographics, '{DateFirstPurchase'):"$" as date_first_purchase
        ,xmlget(demographics, '{BirthDate'):"$" as birth_date
        ,xmlget(demographics, '{MaritalStatus'):"$" as marital_status
        ,xmlget(demographics, '{YearlyIncome'):"$" as yearly_income
        ,xmlget(demographics, '{Gender'):"$" as gender

    from person_raw 
)

select * from person_transformation
