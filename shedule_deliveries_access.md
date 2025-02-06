with 
----------------------------------Uploaded Tables-------------------------------------------------
regional_mapping as (
                    select distinct country,
                    region,
                    sub_region,
                    division,
                    original_territory_id, 
                    new_territory_id,
                    from `kyosk-prod.karuru_upload_tables.territory_region_mapping`
                    ),
new_categories_item_v2 as (
                            select distinct product_bundle_id, 
                            case when company_id = 'KYOSK DIGITAL SERVICES LTD (KE)' then 'KE'
                                when company_id = 'KYOSK DIGITAL SERVICES LIMITED (TZ)' then 'TZ'
                                when company_id = 'KYOSK DIGITAL SERVICES LIMITED (UG)' then 'UG'
                                when company_id = 'KYOSK DIGITAL SOLUTIONS NIGERIA LIMITED ' then 'NG' 
                            else null end as country_code
                            from `kyosk-prod.karuru_upload_tables.new_categories_sku`
                            ), 
uploaded_zero_md_routes_cte as (
                                select distinct country_id,
                                territory_id,
                                route_id,
                                route_name,
                                zero_md_route
                                from `kyosk-prod.karuru_upload_tables.zero_md_routes` 
                                where zero_md_route = true
                                ),
---------------------------- Fulfilment Centers --------------------
fulfillment_center as (
                        select *,
                        row_number()over(partition by id order by updated_at desc) as index 
                        from `kyosk-prod.karuru_reports.fulfillment_center` 
                        where date(created_at) > "2021-06-27" #start date
                        ),
fulfillment_center_cte as (
                            select distinct --date(created_at) created_at,
                            id,
                            name,
                            --country_code,
                            location.latitude,
                            location.longitude
                            from fulfillment_center
                            where index =1 
                            ),
------------------------------ Manage Territory Dashboard Access from erp ---------------------------
report_users as(
                            SELECT DISTINCT user_id,
                            employee_name,
                            kyosk_business_unit,
                            CASE
                              WHEN LOWER(territory) = LOWER("Ruiru") THEN "Ruiru"
                              --WHEN LOWER(territory) = LOWER("Juja") THEN "Ruiru"
                              WHEN LOWER(territory) = LOWER("Ruaka (Kiambu)") THEN "Kiambu"
                              WHEN LOWER(territory) = LOWER("Eastlands (Choppies)") THEN "Eastlands"
                              WHEN LOWER(territory) = LOWER("Mombasa (Majengo)") THEN "Majengo Mombasa"
                              WHEN LOWER(territory) = LOWER("Jos") THEN "Jos-Central"
                              WHEN LOWER(territory) = LOWER("Uyole") THEN "Uyole"
                              WHEN LOWER(territory) = LOWER("Kisumu") THEN "Kisumu1"
                              WHEN LOWER(territory) = LOWER("Igoma") THEN "Igoma"
                              WHEN LOWER(territory) = LOWER("Ajah") THEN "Ajah"
                              WHEN LOWER(territory) = LOWER("Embu") THEN "Embu"
                              WHEN LOWER(territory) = LOWER("Ibadan") THEN "Ibadan_Bodija"
                              WHEN LOWER(territory) = LOWER("Kawempe") THEN "Kawempe"
                              WHEN LOWER(territory) = LOWER("Voi") THEN "Voi"
                              WHEN LOWER(territory) = LOWER("Makindye") THEN "Makindye"
                              WHEN LOWER(territory) = LOWER("Mwenge") THEN "Mwenge"
                              WHEN LOWER(territory) = LOWER("Luzira") THEN "Luzira"
                              WHEN LOWER(territory) = LOWER("Nalukolongo") THEN "Nalukolongo"
                              WHEN LOWER(territory) = LOWER("Ikeja") THEN "Ikeja"
                              ELSE NULL
                            END AS new_territory,
                            ROW_NUMBER()OVER(PARTITION BY id ORDER BY modified DESC) AS row_num
                            FROM `kyosk-prod.karuru_reports.employee`
                            WHERE status = "ACTIVE" AND DATE(creation) >= "2019-01-01"
),
report_users_null_territory AS (
                          SELECT DISTINCT 
                              user_id,
                              employee_name,
                              -- If the new_territory is NULL, assign the predefined list of multiple territories
                              CASE 
                                  WHEN new_territory IS NULL AND kyosk_business_unit = "Global Audit"THEN ["Ruiru", "Ikeja", "Nalukolongo", "Luzira", "Mwenge", "Kawempe", "Voi", "Makindye", "Ibadan_Bodija", "Embu", "Ajah", "Igoma", "Kisumu1", "Uyole", "Jos-Central","Majengo Mombasa", "Eastlands", "Kiambu"]
                                  WHEN new_territory IS NULL AND kyosk_business_unit = "Global Finance" THEN ["Ruiru", "Ikeja", "Nalukolongo", "Luzira", "Mwenge", "Kawempe", "Voi", "Makindye", "Ibadan_Bodija", "Embu", "Ajah", "Igoma", "Kisumu1", "Uyole", "Jos-Central","Majengo Mombasa", "Eastlands", "Kiambu"]
                                  WHEN new_territory IS NULL AND kyosk_business_unit = "Global Data & Strategy" THEN ["Ruiru", "Ikeja", "Nalukolongo", "Luzira", "Mwenge", "Kawempe", "Voi", "Makindye", "Ibadan_Bodija", "Embu", "Ajah", "Igoma", "Kisumu1", "Uyole", "Jos-Central","Majengo Mombasa", "Eastlands", "Kiambu"]
                                  WHEN new_territory IS NULL AND kyosk_business_unit = "Global Tech Product" THEN ["Ruiru", "Ikeja", "Nalukolongo", "Luzira", "Mwenge", "Kawempe", "Voi", "Makindye", "Ibadan_Bodija", "Embu", "Ajah", "Igoma", "Kisumu1", "Uyole", "Jos-Central","Majengo Mombasa", "Eastlands", "Kiambu"]
                                  WHEN new_territory IS NULL AND kyosk_business_unit = "Global" THEN ["Ruiru", "Ikeja", "Nalukolongo", "Luzira", "Mwenge", "Kawempe", "Voi", "Makindye", "Ibadan_Bodija", "Embu", "Ajah", "Igoma", "Kisumu1", "Uyole", "Jos-Central","Majengo Mombasa", "Eastlands", "Kiambu"]
                                  WHEN new_territory IS NULL AND kyosk_business_unit = "Global Tech QA" THEN ["Ruiru", "Ikeja", "Nalukolongo", "Luzira", "Mwenge", "Kawempe", "Voi", "Makindye", "Ibadan_Bodija", "Embu", "Ajah", "Igoma", "Kisumu1", "Uyole", "Jos-Central","Majengo Mombasa", "Eastlands", "Kiambu"]
                                  WHEN new_territory IS NULL AND kyosk_business_unit = "Global Commercial" THEN ["Ruiru", "Ikeja", "Nalukolongo", "Luzira", "Mwenge", "Kawempe", "Voi", "Makindye", "Ibadan_Bodija", "Embu", "Ajah", "Igoma", "Kisumu1", "Uyole", "Jos-Central","Majengo Mombasa", "Eastlands", "Kiambu"]
                                  WHEN new_territory IS NULL AND kyosk_business_unit = "Global Tech Engineering" THEN ["Ruiru", "Ikeja", "Nalukolongo", "Luzira", "Mwenge", "Kawempe", "Voi", "Makindye", "Ibadan_Bodija", "Embu", "Ajah", "Igoma", "Kisumu1", "Uyole", "Jos-Central","Majengo Mombasa", "Eastlands", "Kiambu"]
                                  WHEN new_territory IS NULL AND kyosk_business_unit = "Kenya FMCG" THEN ["Ruiru", "Voi","Embu", "Kisumu1","Majengo Mombasa", "Eastlands", "Kiambu"]
                                  WHEN new_territory IS NULL AND kyosk_business_unit = "Kenya Farm & Fresh" THEN ["Ruiru", "Voi","Embu", "Kisumu1","Majengo Mombasa", "Eastlands", "Kiambu"]
                                  WHEN new_territory IS NULL AND kyosk_business_unit = "Tanzania" THEN ["Mwenge","Uyole","Igoma"]
                                  WHEN new_territory IS NULL AND kyosk_business_unit = "Uganda" THEN ["Nalukolongo","Luzira","Kawempe","Makindye"]
                                  WHEN new_territory IS NULL AND kyosk_business_unit = "Nigeria" THEN ["Ikeja","Ibadan_Bodija","Ajah","Jos-Central"]
                                  ELSE [new_territory] -- Keep the assigned territory if not null
                              END AS new_territory_list
                          FROM report_users
                          WHERE row_num =1

),
----------------------- Promotions Data ----------------------------
so_and_promo_data as (
                      select distinct id,
                      product_bundle_id,
                      promotion_name
                      from `karuru_views.sales_order_and_promotions_mashup`
                      ),
---------------------------------------------------------------------------------------------------
delivery_note_with_index as (
                              select *,                               
                              row_number()over(partition by id order by updated_at desc ) as index
                              from `karuru_reports.delivery_notes` 
                              where territory_id not in ('Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ','DKasarani', 'Test KE Territory', 'Test UG Territory','Test Fresh TZ Territory')
                              and status not in ('EXPIRED','CHANGE_REQUESTED', 'USER_CANCELLED')
                              and date(created_at) >= date_sub(date_trunc(current_date(), month), interval 1 month)
                              --and code = 'DN-KHETIA -EIKT-0HETJVR8X2M9D'
                              ),
scheduled_deliveries_report as (
                                select distinct --dnwi.delivery_window.delivery_date as scheduled_delivery_date,
                                coalesce(dnwi.delivery_window.delivery_date, dnwi.scheduled_delivery_date) as scheduled_delivery_date,
                                date(dnwi.so_created_date) as so_created_date,
                                format_date('%A', coalesce(dnwi.delivery_window.delivery_date, dnwi.scheduled_delivery_date)) as scheduled_delivery_day_of_week,

                                dnwi.country_code,
                                case when fc.name = "Khetia " then 'Khetia' 
                                     when fc.name = 'Kapa Oil Mahitaji FC' then 'Kapa Oil Mahitaji FC'
                                else rm.new_territory_id  end as fullfilment_center_name,
                                rm.region,
                                rm.sub_region,
                                rm.division, 
                                rm.new_territory_id as territory_id,
                                dnwi.route_name,
                                case when dnwi.route_id = uzmdr.route_id then 'YES' else 'NO' end as zero_md_route,  

                                dnwi.code as delivery_note,
                                dnwi.id as delivery_note_id,
                                dnwi.sale_order_id as sales_order, 
                                --sr.sales_code,
                                dnwi.sale_order_code as sales_code,                            	
                                dnwi.delivery_trip_id,
                                dnwi.agent_name,
                                dnwi.status,
                                dnwi.created_on_app, 

                                dnwi.outlet_id,
                                dnwi.outlet.name as outlet_name,
                                dnwi.outlet.phone_number as outlet_phone_number,

                                rup.user_id as user_email,
                                --rup.new_territory_list,
                                
                                oi.item_group_id,
                                oi.product_bundle_id,
                                oi.uom,
                                oi.status as item_status,
                                case when oi.product_bundle_id = nci.product_bundle_id then 'New Category'
                                else 'General Category'
                                end as sku_category,
                                oi.product_bundle_id = nci.product_bundle_id as is_new_category,
                                case when spd.promotion_name is not null then 'YES' else 'NO' end as promo_applied_status,
                                oi.original_item_qty,
                                oi.original_item_qty * oi.discount_amount as discount_amount,
                                oi.total_orderd  - (oi.original_item_qty * oi.discount_amount) as total_ordered,
                                case
                                  when dnwi.status in ('PAID','DELIVERED','CASH_COLLECTED') and oi.status = 'ITEM_FULFILLED' then net_total_delivered
                                else 0 end as gmv_vat_incl
                                from delivery_note_with_index dnwi,unnest(order_items) oi
                                JOIN report_users_null_territory rup ON dnwi.territory_id IN UNNEST(rup.new_territory_list)
                                --left join report_users_null_territory rup on dnwi.territory_id = rup.new_territory
                                left join regional_mapping rm on dnwi.territory_id = rm.original_territory_id
                                left join new_categories_item_v2 as nci on oi.product_bundle_id = nci.product_bundle_id and  dnwi.country_code = nci.country_code
                                left join fulfillment_center_cte fc on dnwi.fullfilment_center_id = fc.id
                                left join so_and_promo_data spd on dnwi.sale_order_id = spd.id and oi.product_bundle_id = spd.product_bundle_id
                                left join uploaded_zero_md_routes_cte uzmdr on dnwi.route_id = uzmdr.route_id
                                where index = 1
                                --and REGEXP_CONTAINS(duwt.user_email,@DS_USER_EMAIL)
                                )
select * from scheduled_deliveries_report WHERE user_email = "essam.muhammad@kyosk.app" 
