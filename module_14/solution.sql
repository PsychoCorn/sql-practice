-- Модуль 14: Свёртывание и наборы группировки

-- Задание 1. ROLLUP — сменный рапорт с подитогами
SELECT
    CASE
        WHEN GROUPING(m.mine_name) = 1 THEN '== ИТОГО =='
        ELSE m.mine_name
    END AS mine,
    CASE
        WHEN GROUPING(s.shift_name) = 1 THEN 'Все смены'
        ELSE s.shift_name
    END AS shift,
    SUM(p.tons_mined) AS total_tons,
    COUNT(DISTINCT p.equipment_id) AS equipment_count
FROM fact_production p
JOIN dim_mine m ON p.mine_id = m.mine_id
JOIN dim_shift s ON p.shift_id = s.shift_id
JOIN dim_date d ON p.date_id = d.date_id
WHERE d.date_id = 20240115
GROUP BY ROLLUP(m.mine_name, s.shift_name)
ORDER BY
    GROUPING(m.mine_name),
    m.mine_name,
    GROUPING(s.shift_name),
    s.shift_name;

-- Задание 2. CUBE — матрица «шахта x тип оборудования»
SELECT
    CASE
        WHEN GROUPING(m.mine_name) = 1 THEN 'ВСЕ ШАХТЫ'
        ELSE m.mine_name
    END AS mine,
    CASE
        WHEN GROUPING(et.type_name) = 1 THEN 'ВСЕ ТИПЫ'
        ELSE et.type_name
    END AS equipment_type,
    SUM(p.tons_mined) AS total_tons,
    ROUND(SUM(p.tons_mined)::numeric / COUNT(DISTINCT p.equipment_id), 2) AS avg_per_equip,
    GROUPING(m.mine_name, et.type_name) AS grouping_level
FROM fact_production p
JOIN dim_mine m ON p.mine_id = m.mine_id
JOIN dim_equipment e ON p.equipment_id = e.equipment_id
JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
JOIN dim_date d ON p.date_id = d.date_id
WHERE d.year = 2024 AND d.quarter = 1
GROUP BY CUBE(m.mine_name, et.type_name)
ORDER BY
    GROUPING(m.mine_name, et.type_name),
    m.mine_name,
    et.type_name;

-- Задание 3. GROUPING SETS — сводка KPI по нескольким срезам
SELECT
    CASE
        WHEN GROUPING(m.mine_name) = 0 THEN 'Шахта'
        WHEN GROUPING(s.shift_name) = 0 THEN 'Смена'
        WHEN GROUPING(et.type_name) = 0 THEN 'Тип оборудования'
        ELSE 'ИТОГО'
    END AS dimension,
    COALESCE(m.mine_name, s.shift_name, et.type_name, 'Все') AS dimension_value,
    SUM(p.tons_mined) AS total_tons,
    SUM(p.trips_count) AS total_trips,
    ROUND(SUM(p.tons_mined)::numeric / NULLIF(SUM(p.trips_count), 0), 2) AS avg_tons_per_trip
FROM fact_production p
JOIN dim_mine m ON p.mine_id = m.mine_id
JOIN dim_shift s ON p.shift_id = s.shift_id
JOIN dim_equipment e ON p.equipment_id = e.equipment_id
JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
JOIN dim_date d ON p.date_id = d.date_id
WHERE d.year = 2024 AND d.month = 1
GROUP BY GROUPING SETS (
    (m.mine_name),
    (s.shift_name),
    (et.type_name),
    ()
)
ORDER BY
    CASE
        WHEN GROUPING(m.mine_name) = 0 THEN 2
        WHEN GROUPING(s.shift_name) = 0 THEN 3
        WHEN GROUPING(et.type_name) = 0 THEN 4
        ELSE 1
    END,
    dimension_value;

-- Задание 4. Условная агрегация — PIVOT (качество руды)
WITH monthly_data AS (
    SELECT
        m.mine_name,
        d.month,
        ROUND(AVG(q.fe_content)::numeric, 2) AS avg_fe_content
    FROM fact_ore_quality q
    JOIN dim_mine m ON q.mine_id = m.mine_id
    JOIN dim_date d ON q.date_id = d.date_id
    WHERE d.year = 2024 AND d.month <= 6
    GROUP BY m.mine_name, d.month
),
pivot_data AS (
    SELECT
        mine_name,
        SUM(CASE WHEN month = 1 THEN avg_fe_content END) AS jan,
        SUM(CASE WHEN month = 2 THEN avg_fe_content END) AS feb,
        SUM(CASE WHEN month = 3 THEN avg_fe_content END) AS mar,
        SUM(CASE WHEN month = 4 THEN avg_fe_content END) AS apr,
        SUM(CASE WHEN month = 5 THEN avg_fe_content END) AS may,
        SUM(CASE WHEN month = 6 THEN avg_fe_content END) AS jun,
        ROUND(AVG(avg_fe_content)::numeric, 2) AS average
    FROM monthly_data
    GROUP BY mine_name
)
SELECT *
FROM (
    SELECT *
    FROM pivot_data

    UNION ALL

    SELECT
        '== ИТОГО ==' AS mine_name,
        ROUND(AVG(jan)::numeric, 2) AS jan,
        ROUND(AVG(feb)::numeric, 2) AS feb,
        ROUND(AVG(mar)::numeric, 2) AS mar,
        ROUND(AVG(apr)::numeric, 2) AS apr,
        ROUND(AVG(may)::numeric, 2) AS may,
        ROUND(AVG(jun)::numeric, 2) AS jun,
        ROUND(AVG(average)::numeric, 2) AS average
    FROM pivot_data
) AS combined_data
ORDER BY mine_name DESC;

-- Задание 5. crosstab — динамический разворот простоев

-- Определяем топ-5 причин простоев
WITH top_reasons AS (
    SELECT dr.reason_name
    FROM dim_downtime_reason dr
    JOIN fact_equipment_downtime fd ON dr.reason_id = fd.reason_id
    JOIN dim_date d ON fd.date_id = d.date_id
    WHERE d.year = 2024 AND d.quarter = 1
    GROUP BY dr.reason_name
    ORDER BY SUM(fd.duration_min) DESC
    LIMIT 5
),
downtime_summary AS (
    SELECT
        e.equipment_name,
        dr.reason_name,
        ROUND(SUM(fd.duration_min) / 60.0, 1) AS total_hours
    FROM fact_equipment_downtime fd
    JOIN dim_equipment e ON fd.equipment_id = e.equipment_id
    JOIN dim_downtime_reason dr ON fd.reason_id = dr.reason_id
    JOIN dim_date d ON fd.date_id = d.date_id
    WHERE d.year = 2024 AND d.quarter = 1
        AND dr.reason_name IN (SELECT reason_name FROM top_reasons)
    GROUP BY e.equipment_name, dr.reason_name
)
SELECT
    equipment_name,
    COALESCE(SUM(CASE WHEN reason_name = 'Плановое техническое обслуживание' THEN total_hours END), 0) AS "Плановое ТО",
    COALESCE(SUM(CASE WHEN reason_name = 'Заправка топливом' THEN total_hours END), 0) AS "Заправка",
    COALESCE(SUM(CASE WHEN reason_name = 'Ожидание транспорта' THEN total_hours END), 0) AS "Ожид. транспорта",
    COALESCE(SUM(CASE WHEN reason_name = 'Отсутствие оператора' THEN total_hours END), 0) AS "Отсут. оператора",
    COALESCE(SUM(CASE WHEN reason_name = 'Ожидание погрузки' THEN total_hours END), 0) AS "Ожид. погрузки",
    ROUND(SUM(total_hours)::numeric, 1) AS "Всего"
FROM downtime_summary
GROUP BY equipment_name
ORDER BY "Всего" DESC;

-- Задание 6. Комплексный отчёт — ROLLUP + PIVOT + итоги
WITH production_data AS (
    SELECT
        m.mine_name,
        d.month,
        SUM(p.tons_mined) AS tons_mined,
        SUM(fd.duration_min) / 60.0 AS downtime_hours
    FROM fact_production p
    JOIN dim_mine m ON p.mine_id = m.mine_id
    JOIN dim_date d ON p.date_id = d.date_id
    LEFT JOIN fact_equipment_downtime fd ON p.equipment_id = fd.equipment_id
        AND p.date_id = fd.date_id
        AND p.shift_id = fd.shift_id
    WHERE d.year = 2024 AND d.quarter = 1
    GROUP BY m.mine_name, d.month
),
production_pivot AS (
    SELECT
        COALESCE(mine_name, '== ИТОГО ==') AS mine,
        'Добыча (тонн)' AS metric,
        SUM(CASE WHEN month = 1 THEN tons_mined END) AS jan,
        SUM(CASE WHEN month = 2 THEN tons_mined END) AS feb,
        SUM(CASE WHEN month = 3 THEN tons_mined END) AS mar,
        SUM(tons_mined) AS q1_total,
        ROUND(
            (SUM(CASE WHEN month = 2 THEN tons_mined END) -
             SUM(CASE WHEN month = 1 THEN tons_mined END)) * 100.0 /
            NULLIF(SUM(CASE WHEN month = 1 THEN tons_mined END), 0), 1
        ) AS feb_vs_jan_pct,
        ROUND(
            (SUM(CASE WHEN month = 3 THEN tons_mined END) -
             SUM(CASE WHEN month = 2 THEN tons_mined END)) * 100.0 /
            NULLIF(SUM(CASE WHEN month = 2 THEN tons_mined END), 0), 1
        ) AS mar_vs_feb_pct,
        CASE
            WHEN ABS(
                (SUM(CASE WHEN month = 3 THEN tons_mined END) -
                 SUM(CASE WHEN month = 2 THEN tons_mined END)) * 100.0 /
                NULLIF(SUM(CASE WHEN month = 2 THEN tons_mined END), 0)
            ) < 5 THEN 'стабильно'
            WHEN SUM(CASE WHEN month = 3 THEN tons_mined END) >
                 SUM(CASE WHEN month = 2 THEN tons_mined END) THEN 'рост'
            ELSE 'снижение'
        END AS trend
    FROM production_data
    GROUP BY ROLLUP(mine_name)
),
downtime_pivot AS (
    SELECT
        COALESCE(mine_name, '== ИТОГО ==') AS mine,
        'Простои (часы)' AS metric,
        ROUND(SUM(CASE WHEN month = 1 THEN downtime_hours END)::numeric, 0) AS jan,
        ROUND(SUM(CASE WHEN month = 2 THEN downtime_hours END)::numeric, 0) AS feb,
        ROUND(SUM(CASE WHEN month = 3 THEN downtime_hours END)::numeric, 0) AS mar,
        ROUND(SUM(downtime_hours)::numeric, 0) AS q1_total,
        ROUND(
            (SUM(CASE WHEN month = 2 THEN downtime_hours END) -
             SUM(CASE WHEN month = 1 THEN downtime_hours END)) * 100.0 /
            NULLIF(SUM(CASE WHEN month = 1 THEN downtime_hours END), 0), 1
        ) AS feb_vs_jan_pct,
        ROUND(
            (SUM(CASE WHEN month = 3 THEN downtime_hours END) -
             SUM(CASE WHEN month = 2 THEN downtime_hours END)) * 100.0 /
            NULLIF(SUM(CASE WHEN month = 2 THEN downtime_hours END), 0), 1
        ) AS mar_vs_feb_pct,
        CASE
            WHEN ABS(
                (SUM(CASE WHEN month = 3 THEN downtime_hours END) -
                 SUM(CASE WHEN month = 2 THEN downtime_hours END)) * 100.0 /
                NULLIF(SUM(CASE WHEN month = 2 THEN downtime_hours END), 0)
            ) < 5 THEN 'стабильно'
            WHEN SUM(CASE WHEN month = 3 THEN downtime_hours END) >
                 SUM(CASE WHEN month = 2 THEN downtime_hours END) THEN 'рост'
            ELSE 'снижение'
        END AS trend
    FROM production_data
    GROUP BY ROLLUP(mine_name)
)
SELECT * FROM (
    SELECT * FROM production_pivot
    UNION ALL
    SELECT * FROM downtime_pivot
) AS combined_report
ORDER BY
    CASE WHEN mine = '== ИТОГО ==' THEN 1 ELSE 0 END,
    mine,
    CASE metric
        WHEN 'Добыча (тонн)' THEN 1
        WHEN 'Простои (часы)' THEN 2
    END;
