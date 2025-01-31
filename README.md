# sql_scripts_kyosk
WITH
----------------------------- Uploaded Tables -----------------------------------
regional_mapping AS (
                    SELECT DISTINCT country, original_territory_id, new_territory_id
                    FROM `kyosk-prod.karuru_upload_tables.territory_region_mapping`
),
---------------------------- Delivery Note Data ---------------------------------
delivery_note_with_index AS (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
                    FROM `karuru_reports.delivery_notes`
                    WHERE DATE(created_at) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 5 MONTH)
),
delivery_note_data AS (
                    SELECT DISTINCT DATE(created_at) AS created_at, id, code, status
                    FROM delivery_note_with_index
                    WHERE rn = 1
                    ),
------------------------------ Filtered Sales Invoice Data ----------------------------------
sales_invoice_filtered AS ( 
                    SELECT * 
                    FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY name ORDER BY modified DESC) AS rn
                    FROM `kyosk-prod.karuru_reports.sales_invoice`
                    WHERE DATE(created) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE, MONTH), INTERVAL 1 MONTH)
                    AND is_karuru_applied = TRUE
                    AND territory_id NOT IN ('Test UG Territory', 'Test NG Territory', 'Kyosk TZ HQ', 'Test TZ Territory', 'Kyosk HQ', 'DKasarani', 'Test KE Territory', 'Test Fresh TZ Territory')
    )
                    WHERE rn = 1
),
---------------------------- Unnest Items as Subquery ---------------------------------
unnest_items AS (
                    SELECT 
                    si.name, 
                    it.item_id, 
                    it.uom,
                    it.item_code,
                    it.item_name, 
                    it.qty,
                    it.item_group_id,
                    it.base_amount,
                    it.base_net_amount, 
                    it.incoming_rate, 
                    it.item_tax_rate
                    FROM sales_invoice_filtered si,
                    UNNEST(si.items) AS it
),
-----------------------------unnest packed items as subquery-------------------------
unnest_packed_items AS(
                    SELECT
                          si.name,
                          pi.parent_item_id,
                          pi.uom,
                          pi.item_code,
                          pi.qty
                          FROM sales_invoice_filtered si,
                          UNNEST(si.packed_items) AS pi
),
---------------------------- Main Query with Unnested Items ---------------------------------
sales_invoice_data AS (
                    SELECT DISTINCT DATE(si.created) AS creation_date,
                    si.name AS invoice_name,
                    rm.new_territory_id AS territory,
                    si.status,
                    si.workflow_state,
                    si.kyosk_delivery_note,
                    dnd.code AS delivery_note,
                    dnd.status AS dn_status,
                    si.grand_total,
                    it.item_code,
                    it.item_name,
                    it.item_group_id,
                    it.uom AS item_uom,
                    it.qty AS item_qty,
                    it.base_amount,
                    it.base_net_amount,
                    pi.item_code AS packed_item_code,
                    pi.uom,
                    pi.parent_item_id,
                    pi.qty AS packed_item_uom,
                    CAST(REGEXP_EXTRACT(it.item_tax_rate, r':\s*([\d.]+)') AS FLOAT64) AS tax_rate
                    FROM sales_invoice_filtered si
                    LEFT JOIN unnest_items it ON si.name = it.name
                    LEFT JOIN unnest_packed_items pi ON it.item_id = pi.parent_item_id AND it.name = pi.name
                    LEFT JOIN regional_mapping rm ON si.territory_id = rm.original_territory_id
                    LEFT JOIN delivery_note_data dnd ON si.kyosk_delivery_note = dnd.id
                    WHERE rm.country = "KENYA"
)
SELECT * FROM sales_invoice_data
WHERE sales_invoice_data.invoice_name = "ACC-SINV-2024-20295"
