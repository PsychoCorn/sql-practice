-- Модуль 13: Использование оконных функций

-- Задание 1. Доля оборудования в общей добыче
SELECT
    e.equipment_name,
    p.tons_mined AS tons,
    SUM(p.tons_mined) OVER() AS total_tons,
    ROUND((p.tons_mined * 100.0 / SUM(p.tons_mined) OVER())::numeric, 1) AS pct
FROM fact_production p
JOIN dim_equipment e ON p.equipment_id = e.equipment_id
WHERE p.date_id = 20240115 AND p.shift_id = 1
ORDER BY p.tons_mined DESC;

-- Задание 2. Нарастающий итог по шахтам
WITH daily_production AS (
    SELECT
        m.mine_name,
        d.full_date,
        SUM(p.tons_mined) AS daily_tons
    FROM fact_production p
    JOIN dim_mine m ON p.mine_id = m.mine_id
    JOIN dim_date d ON p.date_id = d.date_id
    WHERE d.year = 2024 AND d.month = 1
    GROUP BY m.mine_name, d.full_date
)
SELECT
    mine_name,
    full_date,
    daily_tons,
    SUM(daily_tons) OVER(PARTITION BY mine_name ORDER BY full_date) AS running_total
FROM daily_production
ORDER BY mine_name, full_date;

-- Задание 3. Скользящее среднее расхода ГСМ
WITH daily_fuel AS (
    SELECT
        d.full_date,
        SUM(p.fuel_consumed_l) AS daily_fuel
    FROM fact_production p
    JOIN dim_date d ON p.date_id = d.date_id
    WHERE p.mine_id = 1 AND d.year = 2024 AND d.quarter = 1
    GROUP BY d.full_date
)
SELECT
    full_date,
    daily_fuel,
    ROUND(AVG(daily_fuel::numeric) OVER(ORDER BY full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ma_7,
    ROUND(AVG(daily_fuel::numeric) OVER(ORDER BY full_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW), 2) AS ma_14
FROM daily_fuel
ORDER BY full_date;

-- Задание 4. Рейтинг операторов по типам оборудования
-- В PostgreSQL нет QUALIFY, используем CTE + WHERE для фильтрации
WITH operator_stats AS (
    SELECT
        o.last_name || ' ' || LEFT(o.first_name, 1) || '.' AS operator_name,
        et.type_name,
        SUM(p.tons_mined) AS total_tons
    FROM fact_production p
    JOIN dim_operator o ON p.operator_id = o.operator_id
    JOIN dim_equipment e ON p.equipment_id = e.equipment_id
    JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
    JOIN dim_date d ON p.date_id = d.date_id
    WHERE d.year = 2024 AND d.month <= 6
    GROUP BY o.operator_id, o.last_name, o.first_name, et.type_name
),
ranked_operators AS (
    SELECT
        operator_name,
        type_name,
        total_tons,
        RANK() OVER(PARTITION BY type_name ORDER BY total_tons DESC) AS rnk,
        DENSE_RANK() OVER(PARTITION BY type_name ORDER BY total_tons DESC) AS dense_rnk,
        NTILE(4) OVER(PARTITION BY type_name ORDER BY total_tons DESC) AS quartile
    FROM operator_stats
)
SELECT *
FROM ranked_operators
WHERE rnk <= 5
ORDER BY type_name, rnk;

-- Задание 5. Сравнение дневной и ночной смены
-- Используем именованные окна WINDOW для повторного использования
WITH shift_data AS (
    SELECT
        d.full_date,
        s.shift_name,
        s.shift_id,
        SUM(p.tons_mined) AS shift_tons
    FROM fact_production p
    JOIN dim_date d ON p.date_id = d.date_id
    JOIN dim_shift s ON p.shift_id = s.shift_id
    WHERE p.mine_id = 1 AND d.year = 2024 AND d.month = 1
    GROUP BY d.full_date, s.shift_name, s.shift_id
)
SELECT
    full_date,
    shift_name,
    shift_tons,
    LAG(shift_tons) OVER w_seq AS prev_shift,
    ROUND((shift_tons * 100.0 / SUM(shift_tons) OVER(PARTITION BY full_date))::numeric, 1) AS pct_of_day,
    ROUND(AVG(shift_tons::numeric) OVER w7, 1) AS ma_7
FROM shift_data
WINDOW
    w_seq AS (PARTITION BY shift_id ORDER BY full_date),
    w7 AS (PARTITION BY shift_id ORDER BY full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
ORDER BY full_date, shift_id;

-- Задание 6. Интервалы между внеплановыми простоями
WITH downtime_data AS (
    SELECT
        e.equipment_name,
        d.full_date,
        dr.reason_name,
        fd.duration_min,
        LAG(d.full_date) OVER(PARTITION BY fd.equipment_id ORDER BY d.full_date) AS prev_date,
        LEAD(d.full_date) OVER(PARTITION BY fd.equipment_id ORDER BY d.full_date) AS next_date
    FROM fact_equipment_downtime fd
    JOIN dim_date d ON fd.date_id = d.date_id
    JOIN dim_equipment e ON fd.equipment_id = e.equipment_id
    JOIN dim_downtime_reason dr ON fd.reason_id = dr.reason_id
    WHERE fd.is_planned = FALSE AND d.year = 2024 AND d.month <= 6
)
SELECT
    equipment_name,
    full_date,
    reason_name,
    duration_min,
    prev_date,
    CASE WHEN prev_date IS NOT NULL THEN (full_date - prev_date) END AS days_between,
    next_date
FROM downtime_data
ORDER BY equipment_name, full_date;

-- Среднее количество дней между поломками
WITH downtime_intervals AS (
    SELECT
        e.equipment_name,
        d.full_date,
        LAG(d.full_date) OVER(PARTITION BY fd.equipment_id ORDER BY d.full_date) AS prev_date
    FROM fact_equipment_downtime fd
    JOIN dim_date d ON fd.date_id = d.date_id
    JOIN dim_equipment e ON fd.equipment_id = e.equipment_id
    WHERE fd.is_planned = FALSE AND d.year = 2024 AND d.month <= 6
)
SELECT
    equipment_name,
    ROUND(AVG(full_date - prev_date)::numeric, 1) AS avg_days_between,
    COUNT(*) AS total_downtimes
FROM downtime_intervals
WHERE prev_date IS NOT NULL
GROUP BY equipment_name
ORDER BY avg_days_between;

-- Задание 7. Обнаружение выбросов по содержанию Fe методом IQR
WITH fe_stats AS (
    SELECT
        m.mine_name,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY q.fe_content) AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY q.fe_content) AS q3,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY q.fe_content) -
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY q.fe_content) AS iqr
    FROM fact_ore_quality q
    JOIN dim_mine m ON q.mine_id = m.mine_id
    JOIN dim_date d ON q.date_id = d.date_id
    WHERE d.year = 2024 AND d.month <= 6
    GROUP BY m.mine_name
)
SELECT
    m.mine_name,
    d.full_date,
    q.sample_number,
    q.fe_content,
    CASE
        WHEN q.fe_content < fs.q1 - 1.5 * fs.iqr THEN 'Выброс (низ)'
        WHEN q.fe_content > fs.q3 + 1.5 * fs.iqr THEN 'Выброс (верх)'
        ELSE 'Норма'
    END AS status
FROM fact_ore_quality q
JOIN dim_mine m ON q.mine_id = m.mine_id
JOIN dim_date d ON q.date_id = d.date_id
JOIN fe_stats fs ON m.mine_name = fs.mine_name
WHERE d.year = 2024 AND d.month <= 6 AND (q.fe_content < fs.q1 - 1.5 * fs.iqr OR q.fe_content > fs.q3 + 1.5 * fs.iqr)
ORDER BY mine_name, full_date;

-- Подсчет выбросов по шахтам
WITH fe_stats AS (
    SELECT
        m.mine_name,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY q.fe_content) AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY q.fe_content) AS q3,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY q.fe_content) -
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY q.fe_content) AS iqr
    FROM fact_ore_quality q
    JOIN dim_mine m ON q.mine_id = m.mine_id
    JOIN dim_date d ON q.date_id = d.date_id
    WHERE d.year = 2024 AND d.month <= 6
    GROUP BY m.mine_name
)
SELECT
    m.mine_name,
    COUNT(*) FILTER (WHERE q.fe_content < fs.q1 - 1.5 * fs.iqr) AS low_outliers,
    COUNT(*) FILTER (WHERE q.fe_content > fs.q3 + 1.5 * fs.iqr) AS high_outliers,
    COUNT(*) FILTER (WHERE q.fe_content < fs.q1 - 1.5 * fs.iqr OR q.fe_content > fs.q3 + 1.5 * fs.iqr) AS total_outliers
FROM fact_ore_quality q
JOIN dim_mine m ON q.mine_id = m.mine_id
JOIN dim_date d ON q.date_id = d.date_id
JOIN fe_stats fs ON m.mine_name = fs.mine_name
WHERE d.year = 2024 AND d.month <= 6
GROUP BY m.mine_name;

-- Задание 8. ТОП-3 рекордных дня для каждой единицы оборудования
-- Заменяем QUALIFY на CTE + WHERE (PostgreSQL совместимость)
WITH daily_equipment AS (
    SELECT
        e.equipment_name,
        et.type_name,
        d.full_date,
        SUM(p.tons_mined) AS daily_tons
    FROM fact_production p
    JOIN dim_equipment e ON p.equipment_id = e.equipment_id
    JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
    JOIN dim_date d ON p.date_id = d.date_id
    WHERE d.year = 2024
    GROUP BY e.equipment_name, et.type_name, d.full_date
),
ranked_days AS (
    SELECT
        equipment_name,
        type_name,
        full_date,
        daily_tons,
        ROW_NUMBER() OVER(PARTITION BY equipment_name ORDER BY daily_tons DESC) AS record_num,
        FIRST_VALUE(daily_tons) OVER(PARTITION BY equipment_name ORDER BY daily_tons DESC) - daily_tons AS diff_from_top1
    FROM daily_equipment
)
SELECT *
FROM ranked_days
WHERE record_num <= 3
ORDER BY equipment_name, record_num;

-- Задание 9. Парето-анализ причин простоев
WITH reason_stats AS (
    SELECT
        dr.reason_name,
        SUM(fd.duration_min) / 60.0 AS total_hours,
        SUM(fd.duration_min) * 100.0 / SUM(SUM(fd.duration_min)) OVER() AS pct
    FROM fact_equipment_downtime fd
    JOIN dim_downtime_reason dr ON fd.reason_id = dr.reason_id
    JOIN dim_date d ON fd.date_id = d.date_id
    WHERE d.year = 2024 AND d.month <= 6
    GROUP BY dr.reason_name
)
SELECT
    reason_name,
    ROUND(total_hours::numeric, 1) AS total_hours,
    ROUND(pct::numeric, 1) AS pct,
    ROUND(SUM(pct) OVER(ORDER BY total_hours DESC)::numeric, 1) AS cumulative_pct,
    CASE
        WHEN SUM(pct) OVER(ORDER BY total_hours DESC) <= 80 THEN 'A'
        WHEN SUM(pct) OVER(ORDER BY total_hours DESC) <= 95 THEN 'B'
        ELSE 'C'
    END AS pareto_category
FROM reason_stats
ORDER BY total_hours DESC;

-- Задание 10. Дедупликация и обработка повторных записей
-- Оптимизированная версия для PostgreSQL (избегаем множественных подзапросов)
WITH telemetry_with_duplicates AS (
    SELECT
        sensor_id,
        date_id,
        time_id,
        telemetry_id,
        ROW_NUMBER() OVER(PARTITION BY sensor_id, date_id, time_id ORDER BY telemetry_id DESC) AS rn
    FROM fact_equipment_telemetry
),
deduplicated AS (
    SELECT COUNT(*) AS total_after
    FROM telemetry_with_duplicates
    WHERE rn = 1
),
total_counts AS (
    SELECT COUNT(*) AS total_before
    FROM fact_equipment_telemetry
)
SELECT
    tc.total_before,
    d.total_after,
    tc.total_before - d.total_after AS duplicates,
    ROUND(((tc.total_before - d.total_after) * 100.0 / tc.total_before)::numeric, 2) AS duplicate_pct
FROM total_counts tc
CROSS JOIN deduplicated d;

-- Задание 11. Предиктивное обслуживание: обнаружение аномалий в телеметрии
-- Именованные окна WINDOW определены в CTE (PostgreSQL поддерживает)
WITH telemetry_data AS (
    SELECT
        st.type_name AS sensor_type,
        d.full_date,
        t.sensor_value AS value,
        AVG(t.sensor_value::numeric) OVER w8 AS moving_avg,
        STDDEV(t.sensor_value::numeric) OVER w8 AS moving_std,
        (t.sensor_value - LAG(t.sensor_value) OVER w_seq)::numeric AS delta,
        PERCENT_RANK() OVER(PARTITION BY t.sensor_id ORDER BY t.sensor_value) AS pct_rank
    FROM fact_equipment_telemetry t
    JOIN dim_sensor s ON t.sensor_id = s.sensor_id
    JOIN dim_sensor_type st ON s.sensor_type_id = st.sensor_type_id
    JOIN dim_date d ON t.date_id = d.date_id
    JOIN dim_time tm ON t.time_id = tm.time_id
    WHERE t.equipment_id = 1 AND d.full_date BETWEEN '2024-01-01' AND '2024-01-07'
    WINDOW
        w8 AS (PARTITION BY t.sensor_id ORDER BY t.date_id, t.time_id ROWS BETWEEN 7 PRECEDING AND CURRENT ROW),
        w_seq AS (PARTITION BY t.sensor_id ORDER BY t.date_id, t.time_id)
)
SELECT
    sensor_type,
    full_date,
    ROUND(value::numeric, 2) AS value,
    ROUND(moving_avg::numeric, 2) AS moving_avg,
    ROUND(moving_std::numeric, 2) AS moving_std,
    ROUND(delta::numeric, 2) AS delta,
    ROUND(pct_rank::numeric, 4) AS pct_rank,
    CASE
        WHEN pct_rank > 0.95 THEN 'ОПАСНОСТЬ'
        WHEN pct_rank > 0.85 THEN 'ВНИМАНИЕ'
        ELSE 'Норма'
    END AS risk_level
FROM telemetry_data
WHERE pct_rank > 0.85
ORDER BY sensor_type, full_date;

-- Задание 12. Комплексный производственный дашборд
WITH daily_production AS (
    SELECT
        d.full_date,
        SUM(p.tons_mined) AS tons
    FROM fact_production p
    JOIN dim_date d ON p.date_id = d.date_id
    WHERE p.mine_id = 1 AND d.year = 2024 AND d.month = 1
    GROUP BY d.full_date
),
median_calc AS (
    SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tons) AS median_value
    FROM daily_production
)
SELECT
    dp.full_date,
    dp.tons,
    LAG(dp.tons) OVER w_seq AS prev_day,
    ROUND(((dp.tons - LAG(dp.tons) OVER w_seq) * 100.0 / NULLIF(LAG(dp.tons) OVER w_seq, 0))::numeric, 1) AS change_pct,
    ROUND(AVG(dp.tons::numeric) OVER w7, 1) AS ma_7,
    SUM(dp.tons) OVER(ORDER BY dp.full_date) AS running_total,
    RANK() OVER(ORDER BY dp.tons DESC) AS rank,
    CASE NTILE(3) OVER(ORDER BY dp.tons DESC)
        WHEN 1 THEN 'Высокая'
        WHEN 2 THEN 'Средняя'
        WHEN 3 THEN 'Низкая'
    END AS category,
    mc.median_value AS median,
    ROUND(((dp.tons - mc.median_value) * 100.0 / NULLIF(mc.median_value, 0))::numeric, 1) AS med_dev_pct,
    CASE
        WHEN dp.tons - LAG(dp.tons) OVER w_seq > dp.tons * 0.05 THEN 'рост'
        WHEN LAG(dp.tons) OVER w_seq - dp.tons > dp.tons * 0.05 THEN 'снижение'
        ELSE 'стабильно'
    END AS trend
FROM daily_production dp
CROSS JOIN median_calc mc
WINDOW
    w_seq AS (ORDER BY dp.full_date),
    w7 AS (ORDER BY dp.full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
ORDER BY dp.full_date;
