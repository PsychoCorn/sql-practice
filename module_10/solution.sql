-- Задание 1. Скалярный подзапрос — фильтрация (простое)
-- Операторы с добычей выше средней за март 2024
SELECT
    o.last_name || ' ' || LEFT(o.first_name, 1) || '.' AS operator_name,
    SUM(p.tons_mined) AS total_mined,
    (SELECT AVG(sub.total_tons)
     FROM (SELECT SUM(tons_mined) AS total_tons
           FROM fact_production
           WHERE date_id BETWEEN 20240301 AND 20240331
           GROUP BY operator_id) sub) AS avg_production
FROM fact_production p
JOIN dim_operator o ON p.operator_id = o.operator_id
WHERE p.date_id BETWEEN 20240301 AND 20240331
GROUP BY o.operator_id, o.last_name, o.first_name
HAVING SUM(p.tons_mined) > (
    SELECT AVG(sub.total_tons)
    FROM (SELECT SUM(tons_mined) AS total_tons
          FROM fact_production
          WHERE date_id BETWEEN 20240301 AND 20240331
          GROUP BY operator_id) sub
)
ORDER BY total_mined DESC;

-- Задание 2. Многозначный подзапрос с IN (простое)
-- Датчики на оборудовании, участвовавшем в добыче Q1 2024
SELECT
    s.sensor_code,
    st.type_name AS sensor_type,
    e.equipment_name,
    s.status
FROM dim_sensor s
JOIN dim_sensor_type st ON s.sensor_type_id = st.sensor_type_id
JOIN dim_equipment e ON s.equipment_id = e.equipment_id
WHERE s.equipment_id IN (
    SELECT DISTINCT equipment_id
    FROM fact_production
    WHERE date_id BETWEEN 20240101 AND 20240331
)
ORDER BY e.equipment_name, s.sensor_code;

-- Задание 3. NOT IN и ловушка с NULL (среднее)
-- Оборудование без записей о добыче
SELECT
    e.equipment_name,
    et.type_name,
    m.mine_name,
    e.status
FROM dim_equipment e
JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
JOIN dim_mine m ON e.mine_id = m.mine_id
WHERE e.equipment_id NOT IN (
    SELECT equipment_id
    FROM fact_production
    WHERE equipment_id IS NOT NULL
)
ORDER BY e.equipment_name;

-- Задание 4. Коррелированный подзапрос — сравнение внутри группы (среднее)
-- Смены с добычей ниже средней по шахте (Q1 2024, первые 15)
SELECT
    m.mine_name,
    d.full_date,
    e.equipment_name,
    fp.tons_mined,
    ROUND((SELECT AVG(fp2.tons_mined)
          FROM fact_production fp2
          WHERE fp2.mine_id = fp.mine_id
            AND fp2.date_id BETWEEN 20240101 AND 20240331)::numeric, 2) AS mine_avg
FROM fact_production fp
JOIN dim_mine m ON fp.mine_id = m.mine_id
JOIN dim_date d ON fp.date_id = d.date_id
JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
WHERE fp.date_id BETWEEN 20240101 AND 20240331
  AND fp.tons_mined < (
    SELECT AVG(fp2.tons_mined)
    FROM fact_production fp2
    WHERE fp2.mine_id = fp.mine_id
      AND fp2.date_id BETWEEN 20240101 AND 20240331
  )
ORDER BY (fp.tons_mined - (
    SELECT AVG(fp2.tons_mined)
    FROM fact_production fp2
    WHERE fp2.mine_id = fp.mine_id
      AND fp2.date_id BETWEEN 20240101 AND 20240331
)) ASC
LIMIT 15;

-- Задание 5. EXISTS — оборудование с тревожными показаниями (среднее)
-- Оборудование с тревожными показаниями телеметрии за март 2024
SELECT
    e.equipment_name,
    et.type_name,
    m.mine_name,
    (SELECT COUNT(*)
     FROM fact_equipment_telemetry t
     WHERE t.equipment_id = e.equipment_id
       AND t.is_alarm = TRUE
       AND t.date_id BETWEEN 20240301 AND 20240331) AS alarm_count
FROM dim_equipment e
JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
JOIN dim_mine m ON e.mine_id = m.mine_id
WHERE EXISTS (
    SELECT 1
    FROM fact_equipment_telemetry t
    WHERE t.equipment_id = e.equipment_id
      AND t.is_alarm = TRUE
      AND t.date_id BETWEEN 20240301 AND 20240331
)
ORDER BY alarm_count DESC;

-- Задание 6. NOT EXISTS — поиск «пробелов» в данных (среднее)
-- Даты без добычи для equipment_id=1 (март 2024)
SELECT
    d.full_date,
    d.day_of_week_name,
    d.is_weekend
FROM dim_date d
WHERE d.date_id BETWEEN 20240301 AND 20240331
  AND NOT EXISTS (
    SELECT 1
    FROM fact_production p
    WHERE p.equipment_id = 1
      AND p.date_id = d.date_id
  )
ORDER BY d.full_date;

-- Задание 7. Подзапрос с ANY/ALL (среднее)
-- Добыча > ALL самосвалов (оборудование с добычей больше всех самосвалов)
SELECT
    e.equipment_name,
    et.type_name,
    fp.date_id,
    fp.shift_id,
    fp.tons_mined
FROM fact_production fp
JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
WHERE fp.tons_mined > ALL (
    SELECT fp2.tons_mined
    FROM fact_production fp2
    JOIN dim_equipment e2 ON fp2.equipment_id = e2.equipment_id
    JOIN dim_equipment_type et2 ON e2.equipment_type_id = et2.equipment_type_id
    WHERE et2.type_code = 'TRUCK'
)
ORDER BY fp.tons_mined DESC;

-- Задание 8. Коррелированный подзапрос для «последней записи» (сложное)
-- Последняя запись добычи для каждого оборудования
SELECT
    e.equipment_name,
    et.type_name,
    d.full_date,
    fp.tons_mined,
    o.last_name || ' ' || LEFT(o.first_name, 1) || '.' AS operator_name
FROM fact_production fp
JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
JOIN dim_date d ON fp.date_id = d.date_id
JOIN dim_operator o ON fp.operator_id = o.operator_id
WHERE fp.date_id = (
    SELECT MAX(fp2.date_id)
    FROM fact_production fp2
    WHERE fp2.equipment_id = fp.equipment_id
)
ORDER BY d.full_date ASC;

-- Задание 9. Комплексный запрос с вложенными подзапросами (сложное)
-- Среднее время простоев оборудования-передовиков
SELECT
    m.mine_name,
    COUNT(DISTINCT fd.equipment_id) AS top_equipment_count,
    ROUND(AVG(fd.duration_min)::numeric, 1) AS avg_downtime_min,
    ROUND(SUM(fd.duration_min)::numeric / 60, 1) AS total_downtime_hours
FROM fact_equipment_downtime fd
JOIN dim_equipment e ON fd.equipment_id = e.equipment_id
JOIN dim_mine m ON e.mine_id = m.mine_id
WHERE fd.is_planned = FALSE
  AND fd.equipment_id IN (
    SELECT fp.equipment_id
    FROM fact_production fp
    WHERE fp.date_id BETWEEN 20240101 AND 20240331
    GROUP BY fp.equipment_id
    HAVING SUM(fp.tons_mined) > (
        SELECT AVG(total_tons)
        FROM (
            SELECT SUM(tons_mined) AS total_tons
            FROM fact_production
            WHERE date_id BETWEEN 20240101 AND 20240331
            GROUP BY equipment_id
        ) sub
    )
  )
GROUP BY m.mine_name
ORDER BY total_downtime_hours DESC;

-- Задание 10. Подзапрос для расчёта KPI: OEE по оборудованию (продвинутое)
-- Расчет OEE по оборудованию за Q1 2024
SELECT
    e.equipment_name,
    et.type_name,
    ROUND(
        COALESCE(
            (SELECT SUM(fp.operating_hours) FROM fact_production fp
             WHERE fp.equipment_id = e.equipment_id AND fp.date_id BETWEEN 20240101 AND 20240331)
            / NULLIF(
                (SELECT SUM(fp.operating_hours) FROM fact_production fp
                 WHERE fp.equipment_id = e.equipment_id AND fp.date_id BETWEEN 20240101 AND 20240331)
                + (SELECT COALESCE(SUM(fd.duration_min) / 60.0, 0) FROM fact_equipment_downtime fd
                   WHERE fd.equipment_id = e.equipment_id AND fd.date_id BETWEEN 20240101 AND 20240331)
            , 0) * 100
        , 0)::numeric, 1
    ) AS availability_pct,
    ROUND(
        COALESCE(
            (SELECT SUM(fp.tons_mined) FROM fact_production fp
             WHERE fp.equipment_id = e.equipment_id AND fp.date_id BETWEEN 20240101 AND 20240331)
            / NULLIF(
                (SELECT SUM(fp.operating_hours) FROM fact_production fp
                 WHERE fp.equipment_id = e.equipment_id AND fp.date_id BETWEEN 20240101 AND 20240331)
                * et.max_payload_tons
            , 0) * 100
        , 0)::numeric, 1
    ) AS performance_pct,
    ROUND(
        COALESCE(
            (SELECT COUNT(*) FILTER (WHERE q.fe_content >= 55)::numeric / NULLIF(COUNT(*)::numeric, 0)
             FROM fact_ore_quality q
             WHERE q.mine_id = e.mine_id AND q.date_id BETWEEN 20240101 AND 20240331)
            * 100
        , 0)::numeric, 1
    ) AS quality_pct
FROM dim_equipment e
JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
WHERE e.status = 'active'
ORDER BY availability_pct DESC;
