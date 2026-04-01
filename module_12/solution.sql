-- Модуль 12: Использование операторов набора

-- Задание 1. UNION ALL — объединённый журнал событий
SELECT 'Добыча' AS event_type,
       e.equipment_name,
       p.tons_mined AS value,
       'тонн' AS unit
FROM fact_production p
JOIN dim_equipment e ON p.equipment_id = e.equipment_id
WHERE p.date_id = 20240315

UNION ALL

SELECT 'Простой' AS event_type,
       e.equipment_name,
       d.duration_min AS value,
       'мин.' AS unit
FROM fact_equipment_downtime d
JOIN dim_equipment e ON d.equipment_id = e.equipment_id
WHERE d.date_id = 20240315

ORDER BY equipment_name, event_type;

-- Задание 2. UNION — уникальные шахты с активностью
SELECT DISTINCT m.mine_name
FROM fact_production p
JOIN dim_mine m ON p.mine_id = m.mine_id
WHERE p.date_id BETWEEN 20240101 AND 20240331

UNION

SELECT DISTINCT m.mine_name
FROM fact_equipment_downtime d
JOIN dim_equipment e ON d.equipment_id = e.equipment_id
JOIN dim_mine m ON e.mine_id = m.mine_id
WHERE d.date_id BETWEEN 20240101 AND 20240331;

-- Задание 3. EXCEPT — оборудование без данных о качестве
SELECT e.equipment_name,
       et.type_name
FROM dim_equipment e
JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
WHERE e.equipment_id IN (
    SELECT DISTINCT equipment_id
    FROM fact_production
    WHERE date_id BETWEEN 20240101 AND 20240331

    EXCEPT

    SELECT DISTINCT equipment_id
    FROM fact_ore_quality
    WHERE date_id BETWEEN 20240101 AND 20240331
)
ORDER BY e.equipment_name;

-- Задание 4. INTERSECT — операторы на нескольких типах оборудования
SELECT o.last_name || ' ' || o.first_name AS operator_name,
       o.position,
       o.qualification
FROM dim_operator o
WHERE o.operator_id IN (
    SELECT DISTINCT fp.operator_id
    FROM fact_production fp
    JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
    JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
    WHERE et.type_code = 'LHD'

    INTERSECT

    SELECT DISTINCT fp.operator_id
    FROM fact_production fp
    JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
    JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
    WHERE et.type_code = 'TRUCK'
)
ORDER BY operator_name;

-- Задание 5. Диаграмма Венна: комплексный анализ
WITH lhd_operators AS (
    SELECT DISTINCT fp.operator_id
    FROM fact_production fp
    JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
    JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
    WHERE et.type_code = 'LHD'
),
truck_operators AS (
    SELECT DISTINCT fp.operator_id
    FROM fact_production fp
    JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
    JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
    WHERE et.type_code = 'TRUCK'
),
total_operators AS (
    SELECT COUNT(DISTINCT operator_id) AS total_count
    FROM fact_production
)
SELECT 'Оба типа' AS category,
       COUNT(*) AS count,
       ROUND(COUNT(*) * 100.0 / (SELECT total_count FROM total_operators), 1) AS percentage
FROM (
    SELECT operator_id FROM lhd_operators
    INTERSECT
    SELECT operator_id FROM truck_operators
) AS both_types

UNION ALL

SELECT 'Только ПДМ' AS category,
       COUNT(*) AS count,
       ROUND(COUNT(*) * 100.0 / (SELECT total_count FROM total_operators), 1) AS percentage
FROM (
    SELECT operator_id FROM lhd_operators
    EXCEPT
    SELECT operator_id FROM truck_operators
) AS only_lhd

UNION ALL

SELECT 'Только самосвал' AS category,
       COUNT(*) AS count,
       ROUND(COUNT(*) * 100.0 / (SELECT total_count FROM total_operators), 1) AS percentage
FROM (
    SELECT operator_id FROM truck_operators
    EXCEPT
    SELECT operator_id FROM lhd_operators
) AS only_truck;

-- Задание 6. LATERAL — топ-N записей для каждой группы
SELECT m.mine_name,
       dd.full_date,
       e.equipment_name,
       dr.reason_name,
       fd.duration_min,
       ROUND(fd.duration_min / 60.0, 1) AS duration_hours
FROM dim_mine m
CROSS JOIN LATERAL (
    SELECT fd.date_id,
           fd.equipment_id,
           fd.reason_id,
           fd.duration_min
    FROM fact_equipment_downtime fd
    JOIN dim_equipment e ON fd.equipment_id = e.equipment_id
    WHERE e.mine_id = m.mine_id
      AND fd.is_planned = FALSE
      AND fd.date_id BETWEEN 20240101 AND 20240331
    ORDER BY fd.duration_min DESC
    LIMIT 5
) fd
JOIN dim_date dd ON fd.date_id = dd.date_id
JOIN dim_equipment e ON fd.equipment_id = e.equipment_id
JOIN dim_downtime_reason dr ON fd.reason_id = dr.reason_id
WHERE m.status = 'active'
ORDER BY m.mine_name, fd.duration_min DESC;

-- Задание 7. LEFT JOIN LATERAL — последнее показание для каждого датчика
SELECT s.sensor_code,
       st.type_name AS sensor_type,
       e.equipment_name,
       dd.full_date,
       dt.full_time,
       lt.sensor_value,
       lt.is_alarm
FROM dim_sensor s
LEFT JOIN LATERAL (
    SELECT t.date_id,
           t.time_id,
           t.equipment_id,
           t.sensor_value,
           t.is_alarm
    FROM fact_equipment_telemetry t
    WHERE t.sensor_id = s.sensor_id
    ORDER BY t.date_id DESC, t.time_id DESC
    LIMIT 1
) lt ON true
LEFT JOIN dim_sensor_type st ON s.sensor_type_id = st.sensor_type_id
LEFT JOIN dim_equipment e ON lt.equipment_id = e.equipment_id
LEFT JOIN dim_date dd ON lt.date_id = dd.date_id
LEFT JOIN dim_time dt ON lt.time_id = dt.time_id
WHERE s.status = 'active'
ORDER BY lt.date_id ASC NULLS FIRST;

-- Задание 8. UNION ALL + агрегация — сводный KPI-отчёт
WITH kpi_data AS (
    -- Добыча
    SELECT m.mine_name,
           'Добыча (тонн)' AS kpi_name,
           SUM(p.tons_mined) AS kpi_value
    FROM fact_production p
    JOIN dim_mine m ON p.mine_id = m.mine_id
    WHERE p.date_id BETWEEN 20240301 AND 20240331
    GROUP BY m.mine_name

    UNION ALL

    -- Простои
    SELECT m.mine_name,
           'Простои (часы)' AS kpi_name,
           SUM(d.duration_min) / 60.0 AS kpi_value
    FROM fact_equipment_downtime d
    JOIN dim_equipment e ON d.equipment_id = e.equipment_id
    JOIN dim_mine m ON e.mine_id = m.mine_id
    WHERE d.date_id BETWEEN 20240301 AND 20240331
    GROUP BY m.mine_name

    UNION ALL

    -- Качество руды
    SELECT m.mine_name,
           'Среднее Fe (%)' AS kpi_name,
           AVG(q.fe_content) AS kpi_value
    FROM fact_ore_quality q
    JOIN dim_mine m ON q.mine_id = m.mine_id
    WHERE q.date_id BETWEEN 20240301 AND 20240331
    GROUP BY m.mine_name

    UNION ALL

    -- Тревожные показания
    SELECT m.mine_name,
           'Тревожные показания' AS kpi_name,
           COUNT(*) AS kpi_value
    FROM fact_equipment_telemetry t
    JOIN dim_equipment e ON t.equipment_id = e.equipment_id
    JOIN dim_mine m ON e.mine_id = m.mine_id
    WHERE t.date_id BETWEEN 20240301 AND 20240331
      AND t.is_alarm = TRUE
    GROUP BY m.mine_name
)
SELECT mine_name,
       kpi_name,
       ROUND(kpi_value::numeric, 2) AS kpi_value
FROM kpi_data
ORDER BY mine_name, kpi_name;

-- Широкая таблица KPI
WITH kpi_data AS (
    SELECT m.mine_name,
           'Добыча (тонн)' AS kpi_name,
           SUM(p.tons_mined) AS kpi_value
    FROM fact_production p
    JOIN dim_mine m ON p.mine_id = m.mine_id
    WHERE p.date_id BETWEEN 20240301 AND 20240331
    GROUP BY m.mine_name

    UNION ALL

    SELECT m.mine_name,
           'Простои (часы)' AS kpi_name,
           SUM(d.duration_min) / 60.0 AS kpi_value
    FROM fact_equipment_downtime d
    JOIN dim_equipment e ON d.equipment_id = e.equipment_id
    JOIN dim_mine m ON e.mine_id = m.mine_id
    WHERE d.date_id BETWEEN 20240301 AND 20240331
    GROUP BY m.mine_name

    UNION ALL

    SELECT m.mine_name,
           'Среднее Fe (%)' AS kpi_name,
           AVG(q.fe_content) AS kpi_value
    FROM fact_ore_quality q
    JOIN dim_mine m ON q.mine_id = m.mine_id
    WHERE q.date_id BETWEEN 20240301 AND 20240331
    GROUP BY m.mine_name

    UNION ALL

    SELECT m.mine_name,
           'Тревожные показания' AS kpi_name,
           COUNT(*) AS kpi_value
    FROM fact_equipment_telemetry t
    JOIN dim_equipment e ON t.equipment_id = e.equipment_id
    JOIN dim_mine m ON e.mine_id = m.mine_id
    WHERE t.date_id BETWEEN 20240301 AND 20240331
      AND t.is_alarm = TRUE
    GROUP BY m.mine_name
)
SELECT mine_name,
       MAX(CASE WHEN kpi_name = 'Добыча (тонн)' THEN ROUND(kpi_value::numeric, 2) END) AS production_tons,
       MAX(CASE WHEN kpi_name = 'Простои (часы)' THEN ROUND(kpi_value::numeric, 2) END) AS downtime_hours,
       MAX(CASE WHEN kpi_name = 'Среднее Fe (%)' THEN ROUND(kpi_value::numeric, 2) END) AS avg_fe_percent,
       MAX(CASE WHEN kpi_name = 'Тревожные показания' THEN kpi_value::integer END) AS alarm_count
FROM kpi_data
GROUP BY mine_name
ORDER BY mine_name;
