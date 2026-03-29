-- Схема для представлений: kond

-- Установка схемы по умолчанию для создания объектов
SET search_path TO kond, public;

-- Задание 1. Представление — сводка по добыче (простое)
-- Создание представления для ежедневных отчётов по добыче в схеме kond
CREATE OR REPLACE VIEW kond.v_daily_production_summary AS
SELECT
    d.full_date,
    m.mine_name,
    sh.shift_name,
    COUNT(*) AS record_count,
    SUM(p.tons_mined) AS total_tons,
    SUM(p.fuel_consumed_l) AS total_fuel,
    ROUND(AVG(p.trips_count)::numeric, 1) AS avg_trips
FROM public.fact_production p
JOIN public.dim_date d ON p.date_id = d.date_id
JOIN public.dim_mine m ON p.mine_id = m.mine_id
JOIN public.dim_shift sh ON p.shift_id = sh.shift_id
GROUP BY d.full_date, m.mine_name, sh.shift_name
HAVING COUNT(*) > 0;

-- Проверка представления: данные за март 2024, шахта «Северная»
SELECT *
FROM kond.v_daily_production_summary
WHERE full_date BETWEEN '2024-03-01' AND '2024-03-31'
  AND mine_name LIKE '%Северная%'
ORDER BY full_date, shift_name;

-- Задание 2. Представление с ограничением обновления (простое)
-- Создание представления только для внеплановых простоев в схеме kond
CREATE OR REPLACE VIEW kond.v_unplanned_downtime AS
SELECT *
FROM public.fact_equipment_downtime
WHERE is_planned = FALSE
WITH CHECK OPTION;

-- Проверка представления
SELECT COUNT(*) AS total_downtime,
       SUM(CASE WHEN is_planned = FALSE THEN 1 ELSE 0 END) AS unplanned_count,
       SUM(CASE WHEN is_planned = TRUE THEN 1 ELSE 0 END) AS planned_count
FROM public.fact_equipment_downtime;

-- Задание 3. Материализованное представление для качества руды (среднее)
-- Создание материализованного представления для кэширования данных о качестве руды в схеме kond
CREATE MATERIALIZED VIEW IF NOT EXISTS kond.mv_monthly_ore_quality AS
SELECT
    m.mine_name,
    TO_CHAR(d.full_date, 'YYYY-MM') AS year_month,
    COUNT(*) AS sample_count,
    ROUND(AVG(q.fe_content)::numeric, 2) AS avg_fe,
    ROUND(MIN(q.fe_content)::numeric, 2) AS min_fe,
    ROUND(MAX(q.fe_content)::numeric, 2) AS max_fe,
    ROUND(AVG(q.sio2_content)::numeric, 2) AS avg_sio2,
    ROUND(AVG(q.moisture)::numeric, 2) AS avg_moisture
FROM public.fact_ore_quality q
JOIN public.dim_mine m ON q.mine_id = m.mine_id
JOIN public.dim_date d ON q.date_id = d.date_id
GROUP BY m.mine_name, TO_CHAR(d.full_date, 'YYYY-MM')
ORDER BY m.mine_name, year_month;

-- Обновление материализованного представления
REFRESH MATERIALIZED VIEW kond.mv_monthly_ore_quality;

-- Проверка материализованного представления
SELECT *
FROM kond.mv_monthly_ore_quality
ORDER BY mine_name, year_month;

-- Задание 4. Производная таблица — ранжирование операторов (среднее)
-- Лучший оператор каждой смены за Q1 2024
SELECT * FROM (
    SELECT
        sh.shift_name,
        o.last_name || ' ' || LEFT(o.first_name, 1) || '.' AS operator_name,
        SUM(p.tons_mined) AS total_mined,
        ROW_NUMBER() OVER (PARTITION BY p.shift_id ORDER BY SUM(p.tons_mined) DESC) AS rn
    FROM public.fact_production p
    JOIN public.dim_operator o ON p.operator_id = o.operator_id
    JOIN public.dim_shift sh ON p.shift_id = sh.shift_id
    WHERE p.date_id BETWEEN 20240101 AND 20240331
    GROUP BY p.shift_id, sh.shift_name, o.operator_id, o.last_name, o.first_name
) sub
WHERE rn = 1
ORDER BY shift_name;

-- Задание 5. CTE — комплексный отчёт по эффективности (среднее)
-- Доступность оборудования по шахтам за Q1 2024
WITH production_cte AS (
    SELECT
        e.mine_id,
        SUM(p.operating_hours) AS total_operating_hours,
        SUM(p.tons_mined) AS total_tons
    FROM public.fact_production p
    JOIN public.dim_equipment e ON p.equipment_id = e.equipment_id
    WHERE p.date_id BETWEEN 20240101 AND 20240331
    GROUP BY e.mine_id
),
downtime_cte AS (
    SELECT
        e.mine_id,
        SUM(fd.duration_min) / 60.0 AS total_downtime_hours
    FROM public.fact_equipment_downtime fd
    JOIN public.dim_equipment e ON fd.equipment_id = e.equipment_id
    WHERE fd.date_id BETWEEN 20240101 AND 20240331
    GROUP BY e.mine_id
)
SELECT
    m.mine_name,
    ROUND(COALESCE(p.total_operating_hours, 0)::numeric, 1) AS operating_hours,
    ROUND(COALESCE(d.total_downtime_hours, 0)::numeric, 1) AS downtime_hours,
    ROUND(COALESCE(p.total_tons, 0)::numeric, 1) AS total_tons,
    ROUND(
        COALESCE(p.total_operating_hours, 0) /
        NULLIF(COALESCE(p.total_operating_hours, 0) + COALESCE(d.total_downtime_hours, 0), 0) * 100
    ::numeric, 1) AS availability_pct
FROM public.dim_mine m
LEFT JOIN production_cte p ON p.mine_id = m.mine_id
LEFT JOIN downtime_cte d ON d.mine_id = m.mine_id
WHERE m.status = 'active'
ORDER BY availability_pct ASC;

-- Задание 6. Табличная функция — отчёт по простоям (среднее)
-- Создание функции для получения отчёта по простоям оборудования в схеме kond
CREATE OR REPLACE FUNCTION kond.get_equipment_downtime_report(
    p_equipment_id INTEGER,
    p_start_date_id INTEGER,
    p_end_date_id INTEGER
)
RETURNS TABLE (
    full_date DATE,
    reason_name VARCHAR,
    category VARCHAR,
    duration_min NUMERIC,
    duration_hours NUMERIC,
    is_planned BOOLEAN,
    comment TEXT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        d.full_date,
        r.reason_name,
        r.category,
        fd.duration_min,
        ROUND(fd.duration_min / 60.0, 1) AS duration_hours,
        fd.is_planned,
        fd.comment
    FROM public.fact_equipment_downtime fd
    JOIN public.dim_date d ON fd.date_id = d.date_id
    JOIN public.dim_downtime_reason r ON fd.reason_id = r.reason_id
    WHERE fd.equipment_id = p_equipment_id
      AND fd.date_id BETWEEN p_start_date_id AND p_end_date_id
    ORDER BY d.full_date;
END;
$$ LANGUAGE plpgsql;

-- Проверка функции: отчёт по простоям equipment_id=1 за январь 2024
SELECT *
FROM kond.get_equipment_downtime_report(1, 20240101, 20240131);

-- Задание 7. Рекурсивный CTE — иерархия локаций (сложное)
-- Создание таблицы для иерархии локаций в схеме kond (если не существует)
CREATE TABLE IF NOT EXISTS kond.dim_location_hierarchy (
    location_id INTEGER PRIMARY KEY,
    location_name VARCHAR NOT NULL,
    parent_location_id INTEGER,
    level_m NUMERIC,
    location_type VARCHAR,
    FOREIGN KEY (parent_location_id) REFERENCES kond.dim_location_hierarchy(location_id)
);

-- Рекурсивный CTE для получения полной иерархии локаций
WITH RECURSIVE location_hierarchy AS (
    -- Базовый случай: корневые локации (без родителя)
    SELECT
        location_id,
        location_name,
        parent_location_id,
        level_m,
        location_type,
        1 AS depth,
        location_name::TEXT AS path
    FROM kond.dim_location_hierarchy
    WHERE parent_location_id IS NULL

    UNION ALL

    -- Рекурсивный случай: дочерние локации
    SELECT
        lh.location_id,
        lh.location_name,
        lh.parent_location_id,
        lh.level_m,
        lh.location_type,
        rh.depth + 1 AS depth,
        rh.path || ' → ' || lh.location_name AS path
    FROM kond.dim_location_hierarchy lh
    JOIN location_hierarchy rh ON lh.parent_location_id = rh.location_id
)
SELECT
    location_id,
    location_name,
    parent_location_id,
    level_m,
    location_type,
    depth,
    path
FROM location_hierarchy
ORDER BY path;

-- Задание 8. Рекурсивный CTE — генерация календаря и заполнение пропусков (сложное)
-- Рабочие дни без добычи для mine_id=1 в феврале 2024
WITH RECURSIVE dates AS (
    SELECT 20240201 AS date_id
    UNION ALL
    SELECT date_id + 1 FROM dates WHERE date_id < 20240229
)
SELECT
    d.full_date,
    d.day_of_week_name,
    CASE WHEN d.is_weekend THEN 'выходной' ELSE 'рабочий' END AS day_type
FROM dates dt
JOIN public.dim_date d ON dt.date_id = d.date_id
WHERE NOT EXISTS (
    SELECT 1 FROM public.fact_production p
    WHERE p.date_id = dt.date_id
      AND p.mine_id = 1
)
AND d.is_weekend = FALSE
ORDER BY d.full_date;

-- Задание 9. CTE для скользящего среднего (сложное)
-- 7-дневное скользящее среднее добычи для mine_id=1 за Q1 2024
WITH daily_production AS (
    SELECT
        p.date_id,
        d.full_date,
        SUM(p.tons_mined) AS daily_tons
    FROM public.fact_production p
    JOIN public.dim_date d ON p.date_id = d.date_id
    WHERE p.mine_id = 1
      AND p.date_id BETWEEN 20240101 AND 20240331
    GROUP BY p.date_id, d.full_date
)
SELECT
    full_date,
    ROUND(daily_tons::numeric, 1) AS daily_tons,
    ROUND(AVG(daily_tons) OVER (ORDER BY date_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)::numeric, 1) AS moving_avg_7d,
    ROUND(MAX(daily_tons) OVER (ORDER BY date_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)::numeric, 1) AS moving_max_7d,
    ROUND(
        (daily_tons - AVG(daily_tons) OVER (ORDER BY date_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW))
        / NULLIF(AVG(daily_tons) OVER (ORDER BY date_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 0) * 100
    ::numeric, 1) AS deviation_pct,
    CASE
        WHEN ABS(
            (daily_tons - AVG(daily_tons) OVER (ORDER BY date_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW))
            / NULLIF(AVG(daily_tons) OVER (ORDER BY date_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 0) * 100
        ) > 20 THEN 'Аномалия'
        ELSE ''
    END AS anomaly_flag
FROM daily_production
ORDER BY date_id;

-- Задание 10. Комплексное задание: VIEW + CTE + функция (продвинутое)
-- Создание представления для детализации качества руды в схеме kond
CREATE OR REPLACE VIEW kond.v_ore_quality_detail AS
SELECT
    q.quality_id,
    d.full_date,
    m.mine_id,
    m.mine_name,
    sh.shift_name,
    g.grade_name,
    q.fe_content,
    q.sio2_content,
    q.moisture,
    CASE
        WHEN q.fe_content >= 65 THEN 'Богатая'
        WHEN q.fe_content >= 55 THEN 'Средняя'
        WHEN q.fe_content >= 45 THEN 'Бедная'
        ELSE 'Забалансовая'
    END AS quality_category
FROM public.fact_ore_quality q
JOIN public.dim_date d ON q.date_id = d.date_id
JOIN public.dim_mine m ON q.mine_id = m.mine_id
JOIN public.dim_shift sh ON q.shift_id = sh.shift_id
LEFT JOIN public.dim_ore_grade g ON q.ore_grade_id = g.ore_grade_id;

-- Функция для получения сводки по качеству руды с использованием CTE в схеме kond
CREATE OR REPLACE FUNCTION kond.get_ore_quality_summary(
    p_mine_id INTEGER DEFAULT NULL,
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
)
RETURNS TABLE (
    mine_name VARCHAR,
    quality_category VARCHAR,
    sample_count BIGINT,
    avg_fe NUMERIC,
    avg_sio2 NUMERIC,
    avg_moisture NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    WITH filtered_data AS (
        SELECT *
        FROM kond.v_ore_quality_detail v
        WHERE (p_mine_id IS NULL OR v.mine_id = p_mine_id)
          AND (p_start_date IS NULL OR v.full_date >= p_start_date)
          AND (p_end_date IS NULL OR v.full_date <= p_end_date)
    )
    SELECT
        v.mine_name,
        v.quality_category::VARCHAR,
        COUNT(*) AS sample_count,
        ROUND(AVG(v.fe_content)::numeric, 2) AS avg_fe,
        ROUND(AVG(v.sio2_content)::numeric, 2) AS avg_sio2,
        ROUND(AVG(v.moisture)::numeric, 2) AS avg_moisture
    FROM filtered_data v
    GROUP BY v.mine_name, v.quality_category
    ORDER BY v.mine_name,
        CASE v.quality_category
            WHEN 'Богатая' THEN 1
            WHEN 'Средняя' THEN 2
            WHEN 'Бедная' THEN 3
            ELSE 4
        END;
END;
$$ LANGUAGE plpgsql;

-- Проверка функции: сводка по качеству руды
SELECT *
FROM kond.get_ore_quality_summary();

-- Проверка представления
SELECT *
FROM kond.v_ore_quality_detail
ORDER BY full_date DESC
LIMIT 10;

-- Восстановление пути поиска по умолчанию
RESET search_path;
