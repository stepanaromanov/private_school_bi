-- ==========================================================================================================================================
-- Department: EDUCATION
-- Dataset: education_classes_journals_students_attendances__2526
-- Dsecription: Total attendance statistics for each student in the school
-- ==========================================================================================================================================

SELECT
    DATE_TRUNC('day', attendances.attendance_date_timestamp::timestamp) AS date,
    EXTRACT(HOUR FROM attendances.attendance_date_timestamp::timestamp) AS hour,
    CASE WHEN attendances.state = 'attended' THEN 1 ELSE 0 END AS attended,
    CASE WHEN attendances.state = 'not attended' THEN 1 ELSE 0 END AS not_attended,
    CASE WHEN attendances.state = 'reasonable' THEN 1 ELSE 0 END AS reasonable,
    CASE WHEN attendances.state NOT IN ('attended', 'not attended', 'reasonable') THEN 1 ELSE 0 END AS other,
    attendances.id AS attendance_id,
    attendances.lesson_id,
    attendances.comment,
    CASE WHEN attendances.mark = 0 THEN NULL ELSE attendances.mark END AS mark,
    attendances.lesson_date_timestamp,
    attendances.attendance_date_timestamp,
    students.filial,
    students.full_name AS student_id,
    students.full_name,
    students.balance,
    students.gender,
    students.grade AS student_grade,
    students.class___id,
    students.status___id,
    students.status__name,
    students.status__state,
    students.status__is_default,
    students.subscription___id,
    students.subscription__name,
    students.subscription__duration,
    students.subscription__price,
    students.subscription__time_range,
    students.subscription__state,
    students.birthday_timestamp,
    students.contract_date_timestamp,
    classes.grade,
    CONCAT(classes.grade, ' ', classes.section) AS class_full_name,
    CONCAT(classes.head_teacher_first_name, ' ', classes.head_teacher_last_name) AS head_teacher_full_name,
    classes.instruction_language,
    classes.students_count,
    classes.max_students_count,
    classes.head_teacher_phone_number,
    journals.subject_id,
    journals.subject_name,
    journals.journal_id,
    journals.teacher_0_id
FROM education_students__2526 AS students
LEFT JOIN education_attendances__2526 AS attendances ON attendances.student_id = students.id
LEFT JOIN education_classes__2526 AS classes ON students.class___id = classes.id
LEFT JOIN education_attendance_context__2526 AS att_context ON attendances.lesson_id = att_context.id
LEFT JOIN education_journals__2526 AS journals ON journals.subject_id = att_context.subject_id;


-- ==========================================================================================================================================
-- Department: EDUCATION
-- Dataset: education_class_performers_2526
-- Description: Mark performance statistics for each student in the school
-- ==========================================================================================================================================


WITH base AS (
    SELECT
        DATE_TRUNC('month', attendances.attendance_date_timestamp::timestamp) AS month,
        DATE_TRUNC('day', attendances.attendance_date_timestamp::timestamp) AS date,
        CASE WHEN attendances.mark = 0 THEN NULL ELSE attendances.mark END AS mark,
        students.filial,
        students.full_name,
        CASE
            WHEN CONCAT(classes.grade, ' ', classes.section) = ' '
                THEN 'UNKNOWN'
            ELSE CONCAT(classes.grade, ' ', classes.section)
        END AS class_full_name
    FROM education_students__2526 AS students
    LEFT JOIN education_attendances__2526 AS attendances
        ON attendances.student_id = students.id
    LEFT JOIN education_classes__2526 AS classes
        ON students.class___id = classes.id
)
SELECT
    month,
    date,
    filial,
    class_full_name,
    full_name,
    mark,
    ROUND(
        (AVG(mark) FILTER (WHERE mark IS NOT NULL)
         OVER (PARTITION BY full_name, month))::numeric,
        2
    ) AS avg_mark_per_month
FROM base
WHERE class_full_name NOT ILIKE '0%'
  AND class_full_name NOT ILIKE '%PRE-SCHOOL%'
ORDER BY month, filial, class_full_name, full_name, date;



-- ==========================================================================================================================================
-- Department: EDUCATION
-- Dataset: education_students_classes_history_2526
-- Description: Tracks enrollment changes and growth percentage per grade.
-- ==========================================================================================================================================


WITH min_hist AS (
    SELECT 
        DATE(fetched_timestamp) AS fetched_date,
        id,
        MIN(hist_id) AS min_hist_id
    FROM education_students_history__2526
    GROUP BY DATE(fetched_timestamp), id
)
SELECT 
    DATE(h.fetched_timestamp) AS fetched_date,
    h.filial,
    h.grade,
    h.class___id,
    h.status__name,
    h.payment_day,
    h.gender,
    h.id,
    h.full_name,
    h.subscription___id,
    h.subscription__name,
    h.subscription__duration,
    h.subscription__price,
    h.subscription__time_range,
    h.subscription__state,
    h.birthday_timestamp,
    h.contract_date_timestamp,
    h.balance,
    CONCAT(c.grade, ' ', c.section) AS class_name,
    c.students_count,
    c.max_students_count
FROM education_students_history__2526 h
LEFT JOIN min_hist m
    ON m.fetched_date = DATE(h.fetched_timestamp)
    AND m.id = h.id
    AND m.min_hist_id = h.hist_id
LEFT JOIN education_classes__2526 c
    ON c.id = h.class___id



-- ==========================================================================================================================================
-- Department: EDUCATION
-- Dataset: education_classes_history_2526
-- Description: Class Capacity Utilization. Assesses how full each class is relative to its maximum capacity.
-- ==========================================================================================================================================


WITH min_hist AS (
    SELECT 
        DATE(fetched_timestamp) AS fetched_date,
        id,
        MIN(hist_id) AS min_hist_id
    FROM education_classes_history__2526
    GROUP BY DATE(fetched_timestamp), id
)
SELECT 
    m.fetched_date,
    h.filial,
    h.id AS class___id,
    CONCAT(h.grade, ' ', h.section) AS class_name,
    h.students_count,
    CASE WHEN h.max_students_count = 0 THEN 22 ELSE h.max_students_count END AS max_students_count,
    CASE WHEN h.max_students_count = 0 THEN 22 - h.students_count ELSE h.max_students_count - h.students_count END AS difference_students_count
FROM education_classes_history__2526 h
JOIN min_hist m
    ON m.fetched_date = DATE(h.fetched_timestamp)
    AND m.id = h.id
    AND m.min_hist_id = h.hist_id
ORDER BY m.fetched_date, class_name



-- ==========================================================================================================================================
-- Department: EDUCATION
-- Dataset: education_6_months_subscription_forecast
-- Description: Subscription Revenue Forecast. Projects revenue based on current subscriptions.
-- ==========================================================================================================================================



WITH min_hist AS (
    SELECT 
        fetched_timestamp::date AS fetched_date,
        id,
        MIN(hist_id) AS min_hist_id
    FROM education_students_history__2526
    GROUP BY fetched_timestamp::date, id
),
historical_data AS (
    SELECT 
        h.fetched_timestamp::date AS fetched_date,
        h.filial,
        h.grade,
        h.status__name,
        h.payment_day,
        h.gender,
        h.id,
        h.full_name,
        h.subscription___id,
        h.subscription__name,
        h.subscription__duration,
        h.subscription__price,
        h.subscription__time_range,
        h.subscription__state,
        h.birthday_timestamp,
        h.contract_date_timestamp,
        h.balance,
        CONCAT(c.grade, ' ', c.section) AS class_name
    FROM education_students_history__2526 h
    JOIN min_hist m
        ON m.fetched_date = h.fetched_timestamp::date
        AND m.id = h.id
        AND m.min_hist_id = h.hist_id
    LEFT JOIN education_classes__2526 c
        ON c.id = h.class___id
),
latest_dates AS (
    SELECT 
        id,
        MAX(fetched_date) AS latest_fetched_date
    FROM historical_data
    GROUP BY id
),
latest_student_data AS (
    SELECT 
        h.*
    FROM historical_data h
    JOIN latest_dates l
        ON h.id = l.id
        AND h.fetched_date = l.latest_fetched_date
),
future_months AS (
    SELECT 
        generate_series(
            date_trunc('month', CURRENT_DATE) + interval '1 month',
            date_trunc('month', CURRENT_DATE) + interval '6 months',
            interval '1 month'
        )::date AS month_start
)
SELECT 
    to_char(f.month_start, 'YYYY-MM') AS month,
    l.filial,
    l.id,
    l.full_name,
    l.status__name,
    l.subscription__price,
    CASE WHEN subscription__duration::decimal <> 0 AND subscription__duration IS NOT NULL AND l.status__name = 'active' THEN subscription__price::decimal / subscription__duration::decimal ELSE 0 END AS subscription_projected,
    CASE WHEN l.status__name IN ('archive', 'inactive') THEN 2720000 ELSE 0 END AS subscription_lost, -- average subscription price on 16.10.2025
    l.grade,
    l.payment_day,
    l.gender,
    l.subscription___id,
    l.subscription__name,
    l.subscription__duration,
    l.subscription__time_range,
    l.subscription__state,
    l.balance,
    l.class_name
FROM future_months f
CROSS JOIN latest_student_data l
ORDER BY f.month_start, l.id;


-- ==========================================================================================================================================
-- Department: EDUCATION
-- Dataset: education_high_risk_debtors_2526
-- Description: High-Risk Debtors. Students with Overdue Balances, identifies overdue payments with risk levels.
-- ==========================================================================================================================================


SELECT
    s.filial,
    CONCAT(c.grade, ' ', c.section) AS class_name,
    s.full_name,
    s.phone_number,
    s.subscription__duration,
    s.subscription__price,
    s.balance,
    s.balance::DECIMAL / NULLIF(s.subscription__price::DECIMAL / NULLIF(s.subscription__duration::DECIMAL, 0), 0) * 100 AS balance_to_monthly_subscription_price_ratio_percentage
FROM education_students__2526 s
INNER JOIN education_classes__2526 c
ON c.id = s.class___id
WHERE s.balance < 0
AND s.status__state IN ('active', 'new')



-- ==========================================================================================================================================
-- Department: EDUCATION
-- Dataset: education_expected_income_two_months
-- Description: Due Dates Projection for Student Subscriptions.
-- ==========================================================================================================================================


WITH students_monthly AS (
  SELECT *,
         CASE 
           WHEN balance < 0 THEN balance 
           ELSE 0 
         END AS debt,
         subscription__price::DECIMAL / NULLIF(subscription__duration::DECIMAL, 0) AS monthly,
         NULLIF(subscription__price::DECIMAL / NULLIF(subscription__duration::DECIMAL, 0), 0) AS monthly_subscription_price,
         CASE 
           WHEN payment_day IS NULL OR payment_day = 0 THEN 1
           ELSE payment_day
         END AS effective_payment_day
  FROM education_students__2526 
  WHERE status__name IN ('active', 'new')
),
current_info AS (
  SELECT 
    CURRENT_DATE AS today,
    EXTRACT(DAY FROM CURRENT_DATE) AS current_day,
    DATE_TRUNC('month', CURRENT_DATE) AS current_month_start,
    (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month') AS next_month_start,
    (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month - 1 day'::INTERVAL) AS current_month_end,
    ((DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month') + INTERVAL '1 month - 1 day'::INTERVAL) AS next_month_end
)
SELECT
  filial,
  full_name,
  debt,
  due_date,
  CASE 
    WHEN EXTRACT(MONTH FROM due_date) IN (7, 8) THEN 0::DECIMAL 
    ELSE due_amount - debt
  END AS due_amount,
  monthly_subscription_price,
  CASE 
    WHEN payment_day IS NULL OR payment_day = 0 THEN 'Defaulted to 1st - Review'
    ELSE ''
  END AS payment_note
FROM (
  -- Current month remaining due dates (after current day)
  SELECT
    sm.filial,
    sm.full_name,
    sm.debt * -1 AS debt,
    sm.payment_day,
    LEAST(
      (ci.current_month_start + (sm.effective_payment_day - 1) * INTERVAL '1 DAY'),
      ci.current_month_end
    ) AS due_date,
    GREATEST(0::DECIMAL, sm.monthly - sm.balance::DECIMAL) AS due_amount,
    sm.monthly_subscription_price,
    1 AS sort_order
  FROM students_monthly sm
  CROSS JOIN current_info ci
  WHERE sm.effective_payment_day > ci.current_day

  UNION ALL

  -- Next month full due dates (including all <= current_day)
  SELECT 
    sm.filial,
    sm.full_name, 
    sm.debt * -1 AS debt,
    sm.payment_day,
    LEAST(
      (ci.next_month_start + (sm.effective_payment_day - 1) * INTERVAL '1 DAY'),
      ci.next_month_end
    ) AS due_date,
    GREATEST(0::DECIMAL, sm.monthly - CASE 
      WHEN sm.effective_payment_day > ci.current_day THEN GREATEST(0::DECIMAL, sm.balance::DECIMAL - sm.monthly) 
      ELSE sm.balance::DECIMAL 
    END) AS due_amount,
    sm.monthly_subscription_price,
    2 AS sort_order
  FROM students_monthly sm
  CROSS JOIN current_info ci
  WHERE sm.effective_payment_day <= ci.current_day
) AS combined
ORDER BY sort_order, due_date;


-- ==========================================================================================================================================
-- Department: SALES
-- Dataset: sales_leads_dataset
-- Description: Overall lead information
-- ==========================================================================================================================================


WITH leads AS (
SELECT DISTINCT ON (id)
    id,
    name,
    price,
    responsible_user_id,
    group_id,
    status_id,
    pipeline_id,
    loss_reason_id,
    created_by,
    updated_by,
    is_deleted,
    score,
    account_id,
    labor_cost,
    fetched_timestamp,
    created_at_timestamp,
    updated_at_timestamp,
    closed_at_timestamp,
    closest_task_at_timestamp
FROM sales_leads_history_25
ORDER BY id, fetched_timestamp 
DESC
)
SELECT
    l.id,
    l.name,
    l.price,
    responsible.name AS responsible,
    l.group_id,
    CASE WHEN statuses.status_name IS NULL THEN 'UNKNOWN' ELSE statuses.status_name END AS status_name,
    pipelines.name AS pipeline,
    CASE WHEN lossreasons.name IS NULL THEN 'UNKNOWN' ELSE lossreasons.name END AS loss_reason,
    CASE WHEN creator.name IS NULL THEN 'UNKNOWN' ELSE creator.name END AS created_by,
    CASE WHEN updator.name IS NULL THEN 'UNKNOWN' ELSE updator.name END AS updated_by,
    l.is_deleted,
    l.score,
    l.account_id,
    l.labor_cost,
    l.fetched_timestamp,
    l.created_at_timestamp,
    l.updated_at_timestamp,
    l.closed_at_timestamp,
    l.closest_task_at_timestamp
FROM leads l
LEFT JOIN sales_loss_reasons_25 lossreasons ON l.loss_reason_id = lossreasons.id
LEFT JOIN sales_users_25 responsible ON l.responsible_user_id = responsible.id
LEFT JOIN sales_users_25 creator ON l.created_by = creator.id
LEFT JOIN sales_users_25 updator ON l.updated_by = updator.id
LEFT JOIN sales_pipelines_25 pipelines ON pipelines.id = l.pipeline_id
LEFT JOIN sales_statuses_25 statuses ON statuses.status_id = l.status_id


-- ==========================================================================================================================================
-- Department: SALES
-- Dataset: sales_contacts_dataset
-- Description: Overall contact information
-- ==========================================================================================================================================



WITH contacts AS (
SELECT
    id,
    name,
    responsible_user_id,
    created_by,
    updated_by,
    is_deleted,
    is_unsorted,
    embedded,
    fetched_timestamp,
    created_at_timestamp,
    updated_at_timestamp,
    closest_task_at_timestamp
FROM sales_contacts_25
)
SELECT
    c.id,
    c.name,
    responsible.name AS responsible,
    CASE WHEN creator.name IS NULL THEN 'UNKNOWN' ELSE creator.name END AS created_by,
    CASE WHEN updator.name IS NULL THEN 'UNKNOWN' ELSE updator.name END AS updated_by,
    c.is_deleted,
    c.is_unsorted,
    CASE
      WHEN c.embedded LIKE '%name%' THEN
        regexp_replace(c.embedded, '.*name:\s*(.*?)\s*color:.*', '\1')
      ELSE
        'UNKNOWN'
    END AS tag_name,
    c.fetched_timestamp,
    c.created_at_timestamp,
    c.updated_at_timestamp,
    c.closest_task_at_timestamp
FROM contacts c
LEFT JOIN sales_users_25 responsible ON c.responsible_user_id = responsible.id
LEFT JOIN sales_users_25 creator ON c.created_by = creator.id
LEFT JOIN sales_users_25 updator ON c.updated_by = updator.id


-- ==========================================================================================================================================
-- Department: SALES
-- Dataset: sales_tasks_dataset
-- Description: Overall task information
-- ==========================================================================================================================================



WITH tasks AS (
SELECT DISTINCT ON (id)
    id,
    created_by,
    updated_by,
    responsible_user_id,
    entity_type,
    duration,
    is_completed,
    task_type_id,
    result,
    complete_till_timestamp,
    fetched_timestamp,
    created_at_timestamp,
    updated_at_timestamp
FROM sales_tasks_history_25
ORDER BY id, fetched_timestamp 
DESC
)
SELECT
    t.id,
    responsible.name AS responsible,
    CASE WHEN creator.name IS NULL THEN 'UNKNOWN' ELSE creator.name END AS created_by,
    CASE WHEN updator.name IS NULL THEN 'UNKNOWN' ELSE updator.name END AS updated_by,
    t.entity_type,
    t.duration,
    t.is_completed,
    types.code AS task_type,
    t.result,
    t.complete_till_timestamp,
    t.fetched_timestamp,
    t.created_at_timestamp,
    t.updated_at_timestamp
FROM tasks t
LEFT JOIN sales_users_25 responsible ON t.responsible_user_id = responsible.id
LEFT JOIN sales_users_25 creator ON t.created_by = creator.id
LEFT JOIN sales_users_25 updator ON t.updated_by = updator.id
LEFT JOIN sales_task_types_25 types ON t.task_type_id = types.id


-- ==========================================================================================================================================
-- Department: SALES
-- Dataset: sales_responsible_managers_dataset
-- Description: Overall responsible managers information
-- ==========================================================================================================================================



SELECT
    id,
    created_at_timestamp,
        -- Individual name flags
    CASE WHEN LOWER(tag_1) IN ('aysha','aziza','dilrabo','dilnoza','nozima','mohinur','asaloy')
           OR LOWER(tag_2) IN ('aysha','aziza','dilrabo','dilnoza','nozima','mohinur','asaloy')
           OR LOWER(tag_3) IN ('aysha','aziza','dilrabo','dilnoza','nozima','mohinur','asaloy')
           OR LOWER(tag_4) IN ('aysha','aziza','dilrabo','dilnoza','nozima','mohinur','asaloy')
           OR LOWER(tag_5) IN ('aysha','aziza','dilrabo','dilnoza','nozima','mohinur','asaloy')
           OR LOWER(tag_6) IN ('aysha','aziza','dilrabo','dilnoza','nozima','mohinur','asaloy')
           OR LOWER(tag_7) IN ('aysha','aziza','dilrabo','dilnoza','nozima','mohinur','asaloy')
           OR LOWER(tag_8) IN ('aysha','aziza','dilrabo','dilnoza','nozima','mohinur','asaloy')
           OR LOWER(tag_9) IN ('aysha','aziza','dilrabo','dilnoza','nozima','mohinur','asaloy')
           OR LOWER(tag_10) IN ('aysha','aziza','dilrabo','dilnoza','nozima','mohinur','asaloy')
    THEN 0 ELSE 1 END AS unknown,
    -- Separate name columns
    CASE WHEN EXISTS (
        SELECT 1 FROM (VALUES (tag_1), (tag_2), (tag_3), (tag_4), (tag_5),
                             (tag_6), (tag_7), (tag_8), (tag_9), (tag_10)) AS v(tag)
        WHERE LOWER(v.tag) = 'aysha'
    ) THEN 1 ELSE 0 END AS Aysha,

    CASE WHEN EXISTS (
        SELECT 1 FROM (VALUES (tag_1), (tag_2), (tag_3), (tag_4), (tag_5),
                             (tag_6), (tag_7), (tag_8), (tag_9), (tag_10)) AS v(tag)
        WHERE LOWER(v.tag) = 'aziza'
    ) THEN 1 ELSE 0 END AS Aziza,

    CASE WHEN EXISTS (
        SELECT 1 FROM (VALUES (tag_1), (tag_2), (tag_3), (tag_4), (tag_5),
                             (tag_6), (tag_7), (tag_8), (tag_9), (tag_10)) AS v(tag)
        WHERE LOWER(v.tag) = 'dilrabo'
    ) THEN 1 ELSE 0 END AS Dilrabo,

    CASE WHEN EXISTS (
        SELECT 1 FROM (VALUES (tag_1), (tag_2), (tag_3), (tag_4), (tag_5),
                             (tag_6), (tag_7), (tag_8), (tag_9), (tag_10)) AS v(tag)
        WHERE LOWER(v.tag) = 'dilnoza'
    ) THEN 1 ELSE 0 END AS Dilnoza,

    CASE WHEN EXISTS (
        SELECT 1 FROM (VALUES (tag_1), (tag_2), (tag_3), (tag_4), (tag_5),
                             (tag_6), (tag_7), (tag_8), (tag_9), (tag_10)) AS v(tag)
        WHERE LOWER(v.tag) = 'nozima'
    ) THEN 1 ELSE 0 END AS Nozima,

    CASE WHEN EXISTS (
        SELECT 1 FROM (VALUES (tag_1), (tag_2), (tag_3), (tag_4), (tag_5),
                             (tag_6), (tag_7), (tag_8), (tag_9), (tag_10)) AS v(tag)
        WHERE LOWER(v.tag) = 'mohinur'
    ) THEN 1 ELSE 0 END AS Mohinur,

    CASE WHEN EXISTS (
        SELECT 1 FROM (VALUES (tag_1), (tag_2), (tag_3), (tag_4), (tag_5),
                             (tag_6), (tag_7), (tag_8), (tag_9), (tag_10)) AS v(tag)
        WHERE LOWER(v.tag) = 'asaloy'
    ) THEN 1 ELSE 0 END AS Asaloy

FROM sales_leads_25;


-- ==========================================================================================================================================
-- Department: SALES
-- Dataset: sales_custom_fields_tags_errors
-- Description: Fetch the latest fetched_timestamp for custom fields and tags
-- ==========================================================================================================================================


WITH base AS (
SELECT
  'custom_fields' AS table_name,
  cf.entity,
  cf.amocrm_id,
  cf.name,
  MIN(cf.fetched_timestamp) AS fetched_timestamp
FROM sales_custom_fields_history_25 cf
GROUP BY table_name, cf.amocrm_id, cf.name, cf.entity

UNION

SELECT
  'tags' AS table_name,
  t.entity,
  t.amocrm_id,
  t.name,
  MIN(t.fetched_timestamp) AS fetched_timestamp
FROM sales_tags_history_25 t
GROUP BY table_name, t.amocrm_id, t.name, t.entity
)
SELECT
  *
FROM base
WHERE fetched_timestamp > CURRENT_DATE - INTERVAL '1 week'
ORDER BY table_name, amocrm_id, name


-- ==========================================================================================================================================
-- Department: SALES
-- Dataset: sales_inactive_students_last_month
-- Description: Shows students who became inactive last month.
-- ==========================================================================================================================================


SELECT
  MIN(esh.fetched_timestamp)::DATE AS inactive_date,
  esh.grade,
  esh.full_name,
  esh.phone_number,
  esh.contract_number,
  esh.address
FROM education_students_history__2526 esh
WHERE esh.status__name = 'inactive'
GROUP BY
  esh.grade,
  esh.full_name,
  esh.phone_number,
  esh.contract_number,
  esh.address
HAVING
  MIN(esh.fetched_timestamp) > CURRENT_DATE - INTERVAL '1 month'
ORDER BY
  inactive_date DESC


-- ==========================================================================================================================================
-- Department: SALES
-- Dataset: sales_deleted_leads_last_month
-- Description: Shows leads who were deleted from the AmoCRM system last month.
-- ==========================================================================================================================================


SELECT
    MAX(lh.fetched_timestamp)::date AS deleted_date,
    p.name AS project_name,
    CASE WHEN s.status_name IS NULL THEN 'UNKNOWN' ELSE s.status_name END AS status_name,
    lh.name AS lead_name,
    lh.tag_1,
    lh.tag_2,
    lh.tag_3,
    lh.tag_4,
    lh.tag_5,
    CASE WHEN responsible.name IS NULL THEN 'UNKNOWN' ELSE responsible.name END AS responsible,
    CASE WHEN creator.name IS NULL THEN 'UNKNOWN' ELSE creator.name END AS created_by,
    CASE WHEN updator.name IS NULL THEN 'UNKNOWN' ELSE updator.name END AS updated_by,
    CASE WHEN lh.loss_reason_id = 0 THEN 'UNKNOWN' ELSE lossreasons.name END AS loss_reason
FROM sales_leads_history_25 lh
LEFT JOIN sales_users_25 responsible ON lh.responsible_user_id = responsible.id
LEFT JOIN sales_users_25 creator ON lh.created_by = creator.id
LEFT JOIN sales_users_25 updator ON lh.updated_by = updator.id
LEFT JOIN sales_pipelines_25 p ON lh.pipeline_id = p.id
LEFT JOIN sales_statuses_25 s ON lh.status_id = s.status_id
LEFT JOIN sales_loss_reasons_25 lossreasons ON lh.loss_reason_id = lossreasons.id
GROUP BY 2,3,4,5,6,7,8,9,10,11,12,13
HAVING
    MAX(lh.fetched_timestamp)::date < CURRENT_DATE - INTERVAL '1 day' AND
    MAX(lh.fetched_timestamp)::date > CURRENT_DATE - INTERVAL '1 month'
ORDER BY deleted_date DESC, project_name


-- ==========================================================================================================================================
-- Department: FINANCE
-- Dataset: finance_transactions_eduschool
-- Description: Comprehensive information about existing transactions in the EduSchool system, including successful, waiting, cancelled, rejected transactions
-- ==========================================================================================================================================


WITH cashboxes AS (
  SELECT
    DISTINCT cashbox_id, 
    cashbox_name 
  FROM finance_transactions__2526 
)
SELECT
    id,
    organization_id,
    academic_year_id,
    origin,
    transaction_type,
    balance_id,
    branch_id,
    filial,
    transaction_type_name,
    transaction_type_type,
    transaction_type_has_impact_on,
    cb_main.cashbox_name AS cashbox_name,
    cb_to_cashbox.cashbox_name AS to_cashbox_name,
    cb_from_cashbox.cashbox_name AS from_cashbox_name,
    amount,
    payment_method_name,
    payment_type,
    comment,
    CASE WHEN CONCAT(employee_first_name, ' ', employee_last_name) = '0 0' THEN 'UNKNOWN' ELSE CONCAT(employee_first_name, ' ', employee_last_name) END AS employee_name,
    CASE WHEN CONCAT(cashier_first_name, ' ', cashier_last_name) = '0 0' THEN 'UNKNOWN' ELSE CONCAT(cashier_first_name, ' ', cashier_last_name) END AS cashier_name,
    student_full_name,
    parent_transaction_id,
    to_cashbox_id,
    state,
    branch_name,
    actual_date_timestamp,
    fetched_timestamp,
    created_at_timestamp,
    updated_at_timestamp
FROM finance_transactions__2526 ft
INNER JOIN cashboxes cb_main ON ft.cashbox_id = cb_main.cashbox_id
INNER JOIN cashboxes cb_to_cashbox ON ft.to_cashbox_id = cb_to_cashbox.cashbox_id
INNER JOIN cashboxes cb_from_cashbox ON ft.from_cashbox_id = cb_from_cashbox.cashbox_id

-- finance_payout_transactions dataset

select * from finance_transactions__2526 where transaction_type = 'payOut' AND state NOT IN ('rejected', 'cancelled', 'waiting') AND transaction_type_name <> '0'

-- finance_payin_transactions dataset

select * from finance_transactions__2526 where transaction_type = 'payIn' AND state NOT IN ('rejected', 'cancelled', 'waiting') AND transaction_type_name <> '0'

-- finance_transfer_transactions dataset

select * from finance_transactions__2526 where transaction_type = 'transfer' AND state NOT IN ('rejected', 'cancelled', 'waiting') AND transaction_type_name <> '0'


-- finance_payin_payout_transactions dataset

select * from finance_transactions__2526 where transaction_type = 'payOut' AND state NOT IN ('rejected', 'cancelled', 'waiting') AND transaction_type_name <> '0'

UNION

select * from finance_transactions__2526 where transaction_type = 'payIn' AND state NOT IN ('rejected', 'cancelled', 'waiting') AND transaction_type_name <> '0'


-- ==========================================================================================================================================
-- Department: FINANCE
-- Dataset: finance_student_payments_plan_fakt
-- Description: Information for daily and cumulative graphs on planned and actual income from student subscriptions. 
-- ==========================================================================================================================================


WITH base AS (
    SELECT
        filial,
        SUM(
            COALESCE(subscription__price, 0)
            / NULLIF(subscription__duration, 0)
        ) AS monthly_total
    FROM education_students__2526
    WHERE status__name = 'active'
    GROUP BY filial
),
paid AS (
  SELECT
    actual_date_timestamp::DATE AS payment_date,
    s.filial,
    SUM(t.amount) AS paid_amount
    FROM finance_transactions__2526 t
    JOIN education_students__2526 s ON t.student__id = s.id 
    WHERE transaction_type = 'payIn'
    AND state NOT IN ('rejected', 'cancelled', 'waiting')
    AND transaction_type_name = 'OQUVCHI TOLADI'
    GROUP BY actual_date_timestamp::DATE, s.filial
),
months AS (
  SELECT DATE_TRUNC('month', CURRENT_DATE) AS month_start
),
dates AS (
  SELECT m.month_start,
         generate_series(m.month_start, m.month_start + INTERVAL '1 month' - INTERVAL '1 day', '1 day'::interval) AS payment_date
  FROM months m
),
business_days AS (
  SELECT month_start, payment_date,
         ROW_NUMBER() OVER (PARTITION BY month_start ORDER BY payment_date) AS bd_rank
  FROM dates
  WHERE EXTRACT(DOW FROM payment_date) BETWEEN 1 AND 6
),
month_bd_counts AS (
  SELECT month_start, MAX(bd_rank) AS total_bd
  FROM business_days
  GROUP BY month_start
),
payments AS (
  SELECT b.month_start, b.payment_date, b.bd_rank, c.total_bd,
         LEAST(8, c.total_bd) AS first_bd_count,
         CASE WHEN c.total_bd <= 8 THEN 1.0 ELSE 0.5 END AS early_fraction,
         CASE WHEN c.total_bd <= 8 THEN 0.0 ELSE 0.5 END AS late_fraction,
         base.monthly_total,
         base.filial
  FROM business_days b
  JOIN month_bd_counts c ON b.month_start = c.month_start
  CROSS JOIN base
),
calculated AS (
  SELECT
      p.month_start,
      p.filial,
      p.payment_date,
      CASE
        WHEN bd_rank <= first_bd_count THEN (early_fraction * monthly_total) / first_bd_count
        ELSE (late_fraction * monthly_total) / (total_bd - 8)
      END AS amount_to_pay,
      pd.paid_amount
  FROM payments p
  LEFT JOIN paid pd ON pd.payment_date = p.payment_date AND pd.filial = p.filial
)
SELECT
  payment_date,
  filial,
  amount_to_pay::INTEGER AS amount_to_pay,
  paid_amount::INTEGER AS paid_amount,
  (SELECT SUM(amount_to_pay)
   FROM calculated p
   WHERE p.month_start = c.month_start AND p.payment_date <= c.payment_date AND p.filial = c.filial
  )::INTEGER AS cumulative_planned,
  (SELECT SUM(COALESCE(paid_amount, 0))
   FROM calculated p
   WHERE p.month_start = c.month_start AND p.payment_date <= c.payment_date AND p.filial = c.filial
  )::INTEGER AS cumulative_paid
FROM calculated c
ORDER BY filial, payment_date;


-- ==========================================================================================================================================
-- Department: SALES
-- Dataset: sales_budget_errors
-- Description: Displays errors where the responsible manager failed to specify the contract price in the lead information.
-- ==========================================================================================================================================


WITH base AS (
    SELECT DISTINCT ON (id)
        created_at_timestamp::DATE AS created_date,
        price,
        marketing_manba,
        sifatsiz_lid
    FROM sales_leads_history_25
    WHERE status_id IN (
        SELECT status_id
        FROM sales_statuses_25
        WHERE status_name IN ('Shartnoma qilindi', 'Pul toladi')
    ) AND fetched_timestamp IN (
      SELECT fetched_timestamp FROM (
          SELECT id, MAX(fetched_timestamp) AS fetched_timestamp
          FROM sales_leads_history_25
          GROUP BY id
      )
    )
)
SELECT
    b.created_date,
    SUM(CASE WHEN b.price = 0 THEN 1 ELSE 0 END) AS no_price_agreement,
    SUM(CASE WHEN b.price > 0 THEN 1 ELSE 0 END) AS price_agreement
FROM base b
GROUP BY
    b.created_date
ORDER BY b.created_date DESC;


-- ==========================================================================================================================================
-- Department: SALES
-- Dataset: sales_source_quality_errors
-- Description: Displays errors where the responsible manager failed to specify the marketing source or quality reason in the lead information.
-- ==========================================================================================================================================


SELECT DISTINCT ON (id)
    created_at_timestamp::DATE AS created_date,
    CASE WHEN marketing_manba IS NULL THEN 'UNKNOWN' ELSE marketing_manba END AS marketing_manba,
    CASE WHEN sifatsiz_lid IS NULL THEN 'UNKNOWN' ELSE sifatsiz_lid END AS sifatsiz_lid
FROM sales_leads_history_25
WHERE fetched_timestamp IN (
  SELECT fetched_timestamp FROM (
      SELECT id, MAX(fetched_timestamp) AS fetched_timestamp
      FROM sales_leads_history_25
      GROUP BY id
  )
)
GROUP BY id, created_at_timestamp::DATE, marketing_manba, sifatsiz_lid


-- ==========================================================================================================================================
-- Department: IT
-- Dataset: students_churn_analytics
-- Description: Collects information from the system about active and inactive students for later Python machine learning analysis of churn.
-- ==========================================================================================================================================


WITH latest AS (
  SELECT 
      id,
      MAX(fetched_timestamp) AS max_ts
  FROM education_students_history__2526
  GROUP BY id
), 
students AS (
  SELECT h.*
  FROM education_students_history__2526 AS h
  JOIN latest l 
        ON h.id = l.id 
       AND h.fetched_timestamp = l.max_ts
)
SELECT
    students.filial,
    students.id AS student_id,
    students.full_name,
    students.balance,
    students.gender,
    students.status__name,
    CASE 
        WHEN students.birthday_timestamp < '1970-01-02' THEN NULL
        ELSE DATE_PART('year', AGE(CURRENT_DATE, students.birthday_timestamp))
    END AS age_years,


    -- Attendance percentage
    SUM(CASE WHEN attendances.state = 'attended' THEN 1 ELSE 0 END)::float /
    NULLIF(
        SUM(CASE WHEN attendances.state = 'attended' THEN 1 ELSE 0 END) +
        SUM(CASE WHEN attendances.state = 'not attended' THEN 1 ELSE 0 END) +
        SUM(CASE WHEN attendances.state = 'reasonable' THEN 1 ELSE 0 END) +
        SUM(CASE WHEN attendances.state NOT IN ('attended','not attended','reasonable') THEN 1 ELSE 0 END),
    0
    ) AS attended_percentage,

    -- Count marks (non-zero)
    COUNT(NULLIF(attendances.mark, 0)) AS all_marks,

    -- Average marks
    (SUM(attendances.mark)::float / NULLIF(COUNT(NULLIF(attendances.mark, 0)), 0))::numeric(10,2)
        AS average_mark

FROM students
LEFT JOIN education_attendances__2526 AS attendances 
       ON attendances.student_id = students.id
LEFT JOIN education_classes__2526 AS classes 
       ON students.class___id = classes.id
LEFT JOIN education_attendance_context__2526 AS att_context 
       ON attendances.lesson_id = att_context.id
LEFT JOIN education_journals__2526 AS journals 
       ON journals.subject_id = att_context.subject_id

GROUP BY 
    students.filial,
    students.id,
    students.full_name,
    students.balance,
    students.gender,
    students.status__name,
    birthday_timestamp



-- ==========================================================================================================================================
-- Department: FINANCE
-- Dataset: finance_profitability_by_class
-- Description: Projects the profitability of each class for upcoming periods.
-- ==========================================================================================================================================



WITH months AS (
    SELECT to_char(month_date, 'YYYY-MM') AS month
    FROM generate_series(
        date_trunc('month', CURRENT_DATE),   -- start from first day of current month
        date_trunc('month', CURRENT_DATE) + interval '11 month',  -- 12 months ahead
        interval '1 month'
    ) AS month_date
    WHERE EXTRACT(MONTH FROM month_date) NOT IN (7, 8)  -- exclude July & August
)
SELECT
    m.month,
    students.filial,
    students.full_name AS student_id,
    students.full_name,
    students.balance,
    students.gender,
    students.grade AS student_grade,
    students.class___id,
    students.status___id,
    students.status__name,
    students.status__state,
    students.status__is_default,
    students.subscription___id,
    students.subscription__name,
    students.subscription__duration,
    students.subscription__price,
    students.subscription__price / NULLIF(students.subscription__duration, 0) AS subscription_monthly_price,
    students.subscription__time_range,
    students.subscription__state,
    students.birthday_timestamp,
    students.contract_date_timestamp,
    classes.grade,
    CONCAT(classes.grade, ' ', classes.section) AS class_full_name,
    CONCAT(classes.head_teacher_first_name, ' ', classes.head_teacher_last_name) AS head_teacher_full_name,
    classes.instruction_language,
    classes.students_count,
    classes.max_students_count
FROM months m
CROSS JOIN education_students__2526 AS students
LEFT JOIN education_classes__2526 AS classes ON students.class___id = classes.id;



-- ==========================================================================================================================================
-- Department: FINANCE
-- Dataset: finance_school_salaries
-- Description: Detailed information on salaries by employee and role.
-- ==========================================================================================================================================



WITH base AS (
    SELECT 
      origin, 
      amount, 
      payment_type, 
      transaction_type_name, 
      cashier_first_name, 
      cashier_last_name, 
      branch_name, 
      cashbox_name, 
      payment_method_name, 
      filial, 
      employee_first_name, 
      employee_last_name,
      actual_date_timestamp
    FROM finance_transactions__2526 
    WHERE transaction_type = 'payOut' 
    AND state NOT IN ('rejected', 'cancelled', 'waiting') 
    AND transaction_type_name LIKE '%Hodimga%'
)
SELECT
    base.*,
    CASE WHEN ee.type IS NULL THEN 'UNKNOWN' ELSE ee.type END AS employee_type,
    CASE WHEN ee.state IS NULL THEN 'UNKNOWN' ELSE ee.state END AS employee_state,
    CASE WHEN ee.branch_employee__role_name IS NULL THEN 'UNKNOWN' ELSE ee.branch_employee__role_name END AS branch_employee__role_name
FROM base
LEFT JOIN education_employees__2526 ee
ON base.employee_first_name = ee.first_name 
AND base.employee_last_name = ee.last_name


-- ==========================================================================================================================================
-- Department: EDUCATION
-- Dataset: education_classes_journals_students_attendances_last_10_weeks__2526
-- Description: Detailed information on total attendance percentages for the last 70 days and for each of the 10 weeks, for each student.
-- ==========================================================================================================================================


SELECT 
    sub.filial,
    sub.full_name AS student_id,
    sub.full_name,
    sub.balance,
    sub.gender,
    sub.student_grade,
    sub.class___id,
    sub.status___id,
    sub.status__name,
    sub.status__state,
    sub.status__is_default,
    sub.subscription___id,
    sub.subscription__name,
    sub.subscription__duration,
    sub.subscription__price,
    sub.subscription__time_range,
    sub.subscription__state,
    sub.birthday_timestamp,
    sub.contract_date_timestamp,
    sub.grade,
    CONCAT(sub.grade, ' ', sub.section) AS class_full_name,
    CONCAT(sub.head_teacher_first_name, ' ', sub.head_teacher_last_name) AS head_teacher_full_name,
    sub.instruction_language,
    sub.students_count,
    sub.max_students_count,
    sub.head_teacher_phone_number,
    sub.subject_id,
    sub.subject_name,
    sub.journal_id,
    sub.teacher_0_id,
    SUM(CASE WHEN sub.weeks_ago = 1 THEN sub.attended ELSE 0 END) AS attended_week1,
    SUM(CASE WHEN sub.weeks_ago = 1 THEN sub.not_attended ELSE 0 END) AS not_attended_week1,
    SUM(CASE WHEN sub.weeks_ago = 1 THEN sub.reasonable ELSE 0 END) AS reasonable_week1,
    SUM(CASE WHEN sub.weeks_ago = 1 THEN sub.other ELSE 0 END) AS other_week1,
    SUM(CASE WHEN sub.weeks_ago = 2 THEN sub.attended ELSE 0 END) AS attended_week2,
    SUM(CASE WHEN sub.weeks_ago = 2 THEN sub.not_attended ELSE 0 END) AS not_attended_week2,
    SUM(CASE WHEN sub.weeks_ago = 2 THEN sub.reasonable ELSE 0 END) AS reasonable_week2,
    SUM(CASE WHEN sub.weeks_ago = 2 THEN sub.other ELSE 0 END) AS other_week2,
    SUM(CASE WHEN sub.weeks_ago = 3 THEN sub.attended ELSE 0 END) AS attended_week3,
    SUM(CASE WHEN sub.weeks_ago = 3 THEN sub.not_attended ELSE 0 END) AS not_attended_week3,
    SUM(CASE WHEN sub.weeks_ago = 3 THEN sub.reasonable ELSE 0 END) AS reasonable_week3,
    SUM(CASE WHEN sub.weeks_ago = 3 THEN sub.other ELSE 0 END) AS other_week3,
    SUM(CASE WHEN sub.weeks_ago = 4 THEN sub.attended ELSE 0 END) AS attended_week4,
    SUM(CASE WHEN sub.weeks_ago = 4 THEN sub.not_attended ELSE 0 END) AS not_attended_week4,
    SUM(CASE WHEN sub.weeks_ago = 4 THEN sub.reasonable ELSE 0 END) AS reasonable_week4,
    SUM(CASE WHEN sub.weeks_ago = 4 THEN sub.other ELSE 0 END) AS other_week4,
    SUM(CASE WHEN sub.weeks_ago = 5 THEN sub.attended ELSE 0 END) AS attended_week5,
    SUM(CASE WHEN sub.weeks_ago = 5 THEN sub.not_attended ELSE 0 END) AS not_attended_week5,
    SUM(CASE WHEN sub.weeks_ago = 5 THEN sub.reasonable ELSE 0 END) AS reasonable_week5,
    SUM(CASE WHEN sub.weeks_ago = 5 THEN sub.other ELSE 0 END) AS other_week5,
    SUM(CASE WHEN sub.weeks_ago = 6 THEN sub.attended ELSE 0 END) AS attended_week6,
    SUM(CASE WHEN sub.weeks_ago = 6 THEN sub.not_attended ELSE 0 END) AS not_attended_week6,
    SUM(CASE WHEN sub.weeks_ago = 6 THEN sub.reasonable ELSE 0 END) AS reasonable_week6,
    SUM(CASE WHEN sub.weeks_ago = 6 THEN sub.other ELSE 0 END) AS other_week6,
    SUM(CASE WHEN sub.weeks_ago = 7 THEN sub.attended ELSE 0 END) AS attended_week7,
    SUM(CASE WHEN sub.weeks_ago = 7 THEN sub.not_attended ELSE 0 END) AS not_attended_week7,
    SUM(CASE WHEN sub.weeks_ago = 7 THEN sub.reasonable ELSE 0 END) AS reasonable_week7,
    SUM(CASE WHEN sub.weeks_ago = 7 THEN sub.other ELSE 0 END) AS other_week7,
    SUM(CASE WHEN sub.weeks_ago = 8 THEN sub.attended ELSE 0 END) AS attended_week8,
    SUM(CASE WHEN sub.weeks_ago = 8 THEN sub.not_attended ELSE 0 END) AS not_attended_week8,
    SUM(CASE WHEN sub.weeks_ago = 8 THEN sub.reasonable ELSE 0 END) AS reasonable_week8,
    SUM(CASE WHEN sub.weeks_ago = 8 THEN sub.other ELSE 0 END) AS other_week8,
    SUM(CASE WHEN sub.weeks_ago = 9 THEN sub.attended ELSE 0 END) AS attended_week9,
    SUM(CASE WHEN sub.weeks_ago = 9 THEN sub.not_attended ELSE 0 END) AS not_attended_week9,
    SUM(CASE WHEN sub.weeks_ago = 9 THEN sub.reasonable ELSE 0 END) AS reasonable_week9,
    SUM(CASE WHEN sub.weeks_ago = 9 THEN sub.other ELSE 0 END) AS other_week9,
    SUM(CASE WHEN sub.weeks_ago = 10 THEN sub.attended ELSE 0 END) AS attended_week10,
    SUM(CASE WHEN sub.weeks_ago = 10 THEN sub.not_attended ELSE 0 END) AS not_attended_week10,
    SUM(CASE WHEN sub.weeks_ago = 10 THEN sub.reasonable ELSE 0 END) AS reasonable_week10,
    SUM(CASE WHEN sub.weeks_ago = 10 THEN sub.other ELSE 0 END) AS other_week10
FROM (
    SELECT 
        CASE WHEN attendances.state = 'attended' THEN 1 ELSE 0 END AS attended,
        CASE WHEN attendances.state = 'not attended' THEN 1 ELSE 0 END AS not_attended,
        CASE WHEN attendances.state = 'reasonable' THEN 1 ELSE 0 END AS reasonable,
        CASE WHEN attendances.state NOT IN ('attended', 'not attended', 'reasonable') THEN 1 ELSE 0 END AS other,
        CASE WHEN attendances.mark = 0 THEN NULL ELSE attendances.mark END AS mark,
        ((DATE_TRUNC('week', CURRENT_DATE)::date - DATE_TRUNC('week', attendances.attendance_date_timestamp::date)::date) / 7) AS weeks_ago,
        students.filial,
        students.full_name,
        students.balance,
        students.gender,
        students.grade AS student_grade,
        students.class___id,
        students.status___id,
        students.status__name,
        students.status__state,
        students.status__is_default,
        students.subscription___id,
        students.subscription__name,
        students.subscription__duration,
        students.subscription__price,
        students.subscription__time_range,
        students.subscription__state,
        students.birthday_timestamp,
        students.contract_date_timestamp,
        classes.grade,
        classes.section,
        classes.head_teacher_first_name,
        classes.head_teacher_last_name,
        classes.instruction_language,
        classes.students_count,
        classes.max_students_count,
        classes.head_teacher_phone_number,
        journals.subject_id,
        journals.subject_name,
        journals.journal_id,
        journals.teacher_0_id
    FROM education_students__2526 AS students
    LEFT JOIN education_attendances__2526 AS attendances ON attendances.student_id = students.id
    LEFT JOIN education_classes__2526 AS classes ON students.class___id = classes.id
    LEFT JOIN education_attendance_context__2526 AS att_context ON attendances.lesson_id = att_context.id
    LEFT JOIN education_journals__2526 AS journals ON journals.subject_id = att_context.subject_id
    WHERE attendances.attendance_date_timestamp::date BETWEEN (DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '10 weeks') AND CURRENT_DATE
      AND ((DATE_TRUNC('week', CURRENT_DATE)::date - DATE_TRUNC('week', attendances.attendance_date_timestamp::date)::date) / 7) BETWEEN 1 AND 10
) AS sub
GROUP BY 
    sub.filial,
    sub.full_name,
    sub.balance,
    sub.gender,
    sub.student_grade,
    sub.class___id,
    sub.status___id,
    sub.status__name,
    sub.status__state,
    sub.status__is_default,
    sub.subscription___id,
    sub.subscription__name,
    sub.subscription__duration,
    sub.subscription__price,
    sub.subscription__time_range,
    sub.subscription__state,
    sub.birthday_timestamp,
    sub.contract_date_timestamp,
    sub.grade,
    sub.section,
    sub.head_teacher_first_name,
    sub.head_teacher_last_name,
    sub.instruction_language,
    sub.students_count,
    sub.max_students_count,
    sub.head_teacher_phone_number,
    sub.subject_id,
    sub.subject_name,
    sub.journal_id,
    sub.teacher_0_id;


-- Superset queries for the above dataset are also incorporated into the graphs.

/*

SUM(
        reasonable_week1 + reasonable_week2 + reasonable_week3 + reasonable_week4 + reasonable_week5 +
        reasonable_week6 + reasonable_week7 + reasonable_week8 + reasonable_week9 + reasonable_week10 +
        not_attended_week1 + not_attended_week2 + not_attended_week3 + not_attended_week4 + not_attended_week5 +
        not_attended_week6 + not_attended_week7 + not_attended_week8 + not_attended_week9 + not_attended_week10)::FLOAT
    /
    NULLIF(
        SUM(attended_week1 + attended_week2 + attended_week3 + attended_week4 + attended_week5 +
            attended_week6 + attended_week7 + attended_week8 + attended_week9 + attended_week10 +
            reasonable_week1 + reasonable_week2 + reasonable_week3 + reasonable_week4 + reasonable_week5 +
            reasonable_week6 + reasonable_week7 + reasonable_week8 + reasonable_week9 + reasonable_week10 +
            not_attended_week1 + not_attended_week2 + not_attended_week3 + not_attended_week4 + not_attended_week5 +
            not_attended_week6 + not_attended_week7 + not_attended_week8 + not_attended_week9 + not_attended_week10)::FLOAT,
        0
    )::float * 100


(SUM(
        reasonable_week1 + reasonable_week2 + reasonable_week3 + reasonable_week4 + reasonable_week5 +
        reasonable_week6 + reasonable_week7 + reasonable_week8 + reasonable_week9 + reasonable_week10 +
        not_attended_week1 + not_attended_week2 + not_attended_week3 + not_attended_week4 + not_attended_week5 +
        not_attended_week6 + not_attended_week7 + not_attended_week8 + not_attended_week9 + not_attended_week10)::FLOAT
    /
    NULLIF(
        SUM(attended_week1 + attended_week2 + attended_week3 + attended_week4 + attended_week5 +
            attended_week6 + attended_week7 + attended_week8 + attended_week9 + attended_week10 +
            reasonable_week1 + reasonable_week2 + reasonable_week3 + reasonable_week4 + reasonable_week5 +
            reasonable_week6 + reasonable_week7 + reasonable_week8 + reasonable_week9 + reasonable_week10 +
            not_attended_week1 + not_attended_week2 + not_attended_week3 + not_attended_week4 + not_attended_week5 +
            not_attended_week6 + not_attended_week7 + not_attended_week8 + not_attended_week9 + not_attended_week10)::FLOAT,
        0
    )::float * 100) >= 2
    

(SUM(reasonable + not_attended)::float / NULLIF(SUM(reasonable + not_attended + attended)::float, 0) * 100) >= 2
 Filter


SUM(reasonable_week1 + not_attended_week1)::float / NULLIF(SUM(reasonable_week1 + not_attended_week1 + attended_week1)::float, 0)::float * 100

*/


-- ==========================================================================================================================================
-- Department: CEO
-- Dataset: trello
-- Description: Detailed information on Trello boards, including lists, cards, and checklists."
-- ==========================================================================================================================================



WITH cards AS (
SELECT DISTINCT ON (id)
    id,
    board_id,
    list_id,
    card_name,
    closed,
    fetched_timestamp,
    due_date_timestamp
FROM trello_cards_history_25 
ORDER BY id, fetched_timestamp ASC
), checklists AS (
SELECT DISTINCT ON (id)
    id,
    board_id,
    card_id,
    checklist_name,
    item_name,
    state,
    fetched_timestamp
FROM trello_checklists_history_25  
ORDER BY id, fetched_timestamp ASC
)
SELECT
  boards.name AS board_name,
  boards.closed AS board_closed,
  boards.last_activity_timestamp AS board_last_activity,
  boards.last_view_timestamp AS board_last_view,
  lists.list_name,
  lists.closed AS list_closed,
  cards.id AS card_id,
  TO_TIMESTAMP(('x' || SUBSTRING(cards.id FROM 1 FOR 8))::bit(32)::int) AS card_created_at,
  cards.card_name,
  cards.closed AS card_closed,
  cards.fetched_timestamp AS card_fetched,
  cards.due_date_timestamp AS card_due_date,
  checklists.id AS checklist_id,
  checklists.checklist_name,
  checklists.item_name,
  checklists.state AS checklist_state,
  checklists.fetched_timestamp AS checklist_fetched
FROM trello_boards_25 boards
LEFT JOIN trello_lists_25 lists ON boards.id = lists.board_id
LEFT JOIN cards ON boards.id = cards.board_id AND cards.list_id = lists.id
LEFT JOIN checklists ON boards.id = checklists.board_id AND cards.id = checklists.card_id


-- ==========================================================================================================================================
-- Department: EDUCATION
-- Dataset: student_health_score
-- Description: Detailed information on total attendance percentages for the last 70 days and for each of the 10 weeks, for each student.

/*

20.11.2025 ML churn analysis 

Data description (example):

            balance         attended_percentage     all_marks       average_mark

count       6.280000e+02    628.000000              628.000000      619.000000
mean        -3.811917e+05   0.942190                54.181529       4.591454
std         2.246799e+06    0.075745                33.334975       0.367276
min         -6.500000e+06   0.125000                0.000000        2.880000
25%         -1.626250e+06   0.914384                28.000000       4.400000
50%         0.000000e+00    0.965825                52.000000       4.670000
75%         0.000000e+00    0.989672                77.000000       4.890000
max         1.979500e+07    1.000000                163.000000      5.000000


Feature importance:

- Attendance 50.1%
- All marks 25.2%
- Average mark 13.9%
- Balance 10.8%


LINEAR NORMALIZATION FORMULA

Input range: [min_x, max_x]
Output range: [min_y, max_y]


Formula derivation:

1. General normalization:
   y = (x - min_x) * (max_y - min_y) / (max_x - min_x) + min_y

2. Plug values (example):

Goal:

   student all mark (x) = 102

   min_x = 0
   max_x = 163
   min_y = 0
   max_y = 0.252

3. Simplify:
   y = (102 - 0) * 0.252 / (163 - 0) + 0

4. Final result :
   y =  0.1577

 AS normalized_all_marks_score


*/ 

-- ==========================================================================================================================================


WITH latest AS (
    SELECT 
        id,
        MAX(fetched_timestamp) AS max_ts
    FROM education_students_history__2526
    GROUP BY id
), 
students AS (
    SELECT h.*
    FROM education_students_history__2526 AS h
    JOIN latest l 
          ON h.id = l.id 
         AND h.fetched_timestamp = l.max_ts
), student_metrics AS (
    SELECT
        students.filial,
        classes.grade,
        students.id AS student_id,
        CONCAT(students.first_name, ' ', students.last_name) AS student_name,
        students.balance::NUMERIC(10,2) AS balance,
        students.gender,
        students.status__name,
        CASE 
            WHEN students.birthday_timestamp < '1970-01-02'
                THEN classes.grade + 6
            ELSE DATE_PART('year', AGE(CURRENT_DATE, students.birthday_timestamp))
        END AS age_years,
        (SUM(CASE WHEN attendances.state = 'attended' THEN 1 ELSE 0 END)::float /
         NULLIF(
            SUM(CASE WHEN attendances.state IN ('attended','not attended','reasonable') THEN 1 ELSE 0 END) +
            SUM(CASE WHEN attendances.state NOT IN ('attended','not attended','reasonable') THEN 1 ELSE 0 END),
         0)
        )::NUMERIC(10,2) AS attended_percentage,
        COUNT(NULLIF(attendances.mark,0)) AS all_marks,
        COALESCE(
            (SUM(attendances.mark)::float / NULLIF(COUNT(NULLIF(attendances.mark,0)),0))::NUMERIC(10,2),
            AVG(SUM(attendances.mark)::float / NULLIF(COUNT(NULLIF(attendances.mark,0)),0)) OVER ()
        ) AS average_mark
    FROM students
    LEFT JOIN education_attendances__2526 AS attendances 
           ON attendances.student_id = students.id
    LEFT JOIN education_classes__2526 AS classes 
           ON students.class___id = classes.id
    LEFT JOIN education_attendance_context__2526 AS att_context 
           ON attendances.lesson_id = att_context.id
    LEFT JOIN education_journals__2526 AS journals 
           ON journals.subject_id = att_context.subject_id
    WHERE students.status__name = 'active'
      AND attendances.lesson_date_timestamp > current_date - interval '70 days'
    GROUP BY 
        students.filial,
        classes.grade,
        students.id,
        CONCAT(students.first_name, ' ', students.last_name),
        students.balance,
        students.gender,
        students.status__name,
        birthday_timestamp
), minmax AS (
    SELECT
        MIN(attended_percentage) AS min_attendance,
        MAX(attended_percentage) AS max_attendance,
        MIN(all_marks) AS min_all_marks,
        MAX(all_marks) AS max_all_marks,
        MIN(average_mark) AS min_average_mark,
        MAX(average_mark) AS max_average_mark,
        MIN(balance) AS min_balance,
        MAX(balance) AS max_balance
    FROM student_metrics
)
SELECT
    sm.*,
    mm.*,
    
    -- Normalize features to [0,1] and multiply by ML weight

    CASE 
        WHEN mm.max_attendance = mm.min_attendance THEN 0.501::NUMERIC(10,2)
        ELSE (((sm.attended_percentage - mm.min_attendance) / NULLIF(mm.max_attendance - mm.min_attendance,0)) * 0.501)::NUMERIC(10,2)
    END AS attendance_score,
    
    CASE 
        WHEN mm.max_all_marks = mm.min_all_marks THEN 0.252::NUMERIC(10,2)
        ELSE (((sm.all_marks::float - mm.min_all_marks::float) / NULLIF(mm.max_all_marks::float - mm.min_all_marks::float,0)) * 0.252)::NUMERIC(10,2)
    END AS all_marks_score,
    
    CASE 
        WHEN mm.max_average_mark = mm.min_average_mark THEN 0.139::NUMERIC(10,2)
        ELSE (((sm.average_mark - mm.min_average_mark) / NULLIF(mm.max_average_mark - mm.min_average_mark,0)) * 0.139)::NUMERIC(10,2)
    END AS average_mark_score,
    
    CASE 
        WHEN mm.max_balance = mm.min_balance THEN 0.108::NUMERIC(10,2)
        ELSE (((sm.balance - mm.min_balance) / NULLIF(mm.max_balance - mm.min_balance,0)) * 0.108)::NUMERIC(10,2)
    END AS balance_score

FROM student_metrics sm
CROSS JOIN minmax mm
ORDER BY filial, health_score ASC;


-- ==========================================================================================================================================
-- Department: FINANCE
-- Dataset: finance_classes_paid_current_month
-- Description: Detailed information on total amount paid in current month for each class
-- ==========================================================================================================================================


WITH subscriptions AS (
  SELECT
      students.filial,
      CONCAT(classes.grade, ' ', classes.section) AS class_full_name,
      CASE WHEN students.balance < 0 THEN students.balance ELSE 0 END AS debt,
      students.subscription__price / NULLIF(students.subscription__duration, 0) AS subscription_monthly_price
  FROM education_students__2526 AS students
  JOIN education_classes__2526 AS classes ON students.class___id = classes.id
), paid AS (
  SELECT
      s.filial,
      CONCAT(classes.grade, ' ', classes.section) AS class_full_name,
      SUM(t.amount) AS paid_amount
  FROM finance_transactions__2526 t
  JOIN education_students__2526 s ON t.student__id = s.id
  JOIN education_classes__2526 AS classes ON s.class___id = classes.id
  WHERE transaction_type = 'payIn'
  AND state NOT IN ('rejected', 'cancelled', 'waiting')
  AND transaction_type_name = 'OQUVCHI TOLADI'
  AND EXTRACT(MONTH FROM actual_date_timestamp) = EXTRACT(MONTH FROM CURRENT_DATE)
  AND EXTRACT(YEAR  FROM actual_date_timestamp) = EXTRACT(YEAR  FROM CURRENT_DATE)
  GROUP BY s.filial, CONCAT(classes.grade, ' ', classes.section)
)
SELECT
    subscriptions.filial,
    subscriptions.class_full_name,
    paid.paid_amount,
    SUM(subscriptions.debt) AS debt,
    SUM(subscriptions.subscription_monthly_price) AS due_amount,
    (paid.paid_amount::float * 100 / SUM(subscriptions.subscription_monthly_price)::float)::numeric(10,2) AS paid_percentage,
    (SUM(subscriptions.debt)::float * -100 / SUM(subscriptions.subscription_monthly_price)::float)::numeric(10,2) AS debt_percentage
FROM subscriptions
JOIN paid
ON subscriptions.filial = paid.filial
AND subscriptions.class_full_name = paid.class_full_name
GROUP BY 1,2,3
    

-- ==========================================================================================================================================
-- Department: FINANCE
-- Dataset: finance_school_costs_percentage_from_total_monthly_income
-- Description: Detailed information on total amount paid in current month for each class
-- ==========================================================================================================================================



WITH payin AS (
  SELECT
      filial,
      date_trunc('month', actual_date_timestamp) AS month,
      SUM(amount)::FLOAT AS total_monthly_income
  FROM finance_transactions__2526 
  WHERE transaction_type = 'payIn' 
  AND state NOT IN ('rejected', 'cancelled', 'waiting')
  AND actual_date_timestamp > '2025-01-01'
  AND transaction_type_name = 'OQUVCHI TOLADI'
  GROUP BY 1,2
), payout AS (
  SELECT
      filial,
      date_trunc('month', actual_date_timestamp) AS month,
      transaction_type_name,
      SUM(amount)::FLOAT AS total_cost
  FROM finance_transactions__2526 
  WHERE transaction_type = 'payOut' 
  AND state NOT IN ('rejected', 'cancelled', 'waiting')
  AND actual_date_timestamp > '2025-01-01'
  AND transaction_type_name <> '0'
  GROUP BY 1,2,3
)
SELECT
  p1.*,
  p2.transaction_type_name,
  p2.total_cost,
  (p2.total_cost * 100 / p1.total_monthly_income)::NUMERIC(10,2) AS cost_percentage
FROM payin p1
JOIN payout p2
ON p1.month = p2.month
AND p1.filial = p2.filial
ORDER BY p1.month, p1.filial


-- ==========================================================================================================================================
-- Department: FINANCE
-- Dataset: finance_school_food_codes
-- Description: Detailed information about lunch costs food codes, implemented since 2025-12-08

-- ==========================================================================================================================================


select 
  *,
  CASE 
    WHEN comment = '100' THEN 'meat'
    WHEN comment = '200' THEN 'vegetables'
    WHEN comment = '300' THEN 'fruits'
    WHEN comment = '400' THEN 'grains'
    WHEN comment = '500' THEN 'dairy products'
    WHEN comment = '600' THEN 'baked goods'
    WHEN comment = '700' THEN 'spices, oils'
    WHEN comment = '800' THEN 'other'
    WHEN comment = '900' THEN 'cleaning' 
  ELSE 'WRONG CODE' END AS food_code
FROM finance_transactions__2526
WHERE transaction_type = 'payOut'
AND state NOT IN ('rejected', 'cancelled', 'waiting')
AND transaction_type_name = 'Abet xarajat'
AND actual_date_timestamp > '2025-12-08'


-- ==========================================================================================================================================
-- Department: FINANCE
-- Dataset: finance_school_costs_general_categories
-- Description: Detailed information about grouped general categories

-- ==========================================================================================================================================

select 
  *,
  CASE 
    WHEN transaction_type_name IN ('Dividend Xushnudbek J. (Zeta)', 'Divident Xushnudbek J. (Zeta)', 'Divident', 'Litsenziya uchun', 'Qarzlarni uzish (uskuna va jihoz)', 'Qarzlarni uzish ( uskuna va jihoz )','Yangi uskuna sotib olish') THEN 'Dividentlar va investorlar'
    WHEN transaction_type_name IN ('Back ofisga pul berish', 'Bonus (maktabga oquvchi olib kelgan shaxslar uchun)', 'Marketing harajat', 'Maktabga oquvchi olib kelgan shaxslar uchun bonus', 'Maktabga ooquvchi olib galgan shaxslar uchun bonus', '(agar xohlasangiz shu yerga ham otkazish mumkin)', 'HR xarajatlari (recruiting)', 'HR xarajatlari (recruting)') THEN 'Back ofis'
    WHEN transaction_type_name IN ('KITOB UCHUN', 'Oqitish xarajatlari', 'Welcome pack', 'Stependiya uchun') THEN 'Talim va oquv materiallari'
    WHEN transaction_type_name IN ('Ijara', 'Ofis harajatlari','Offise harajatlari', 'Komunal (svet)', 'Komunal(svet)', 'Internet va telefon', 'Bank usluglari', 'Kanstavar', 'Tamirlash xarajatlari') THEN 'Ofis va operatsion xarajatlar'
    WHEN transaction_type_name IN ('Tadbiriy harajatlar') THEN 'Tadbirlar'
    WHEN transaction_type_name IN ('Transport xarajati') THEN 'Logistika'
    WHEN transaction_type_name IN ('SOLIQLAR') THEN 'Soliq va majburiy tolovlar'
    WHEN transaction_type_name IN ('Pulni qaytarish', 'Adashib kiritilgan pulni balansdan chiqarish') THEN 'Moliyaviy tuzatishlar'
    WHEN transaction_type_name IN ('Abet xarajat') THEN 'Abet xarajat'
    WHEN transaction_type_name IN ('Hodimga oylik', 'Hodimga avans', 'Xodimga bonus', 'Bongi uchun') THEN 'Ish xaqi'
  ELSE 'OTHER' END AS category
FROM finance_transactions__2526
WHERE transaction_type = 'payOut'
AND state NOT IN ('rejected', 'cancelled', 'waiting')

