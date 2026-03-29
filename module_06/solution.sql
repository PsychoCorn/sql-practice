-- Задание 1. Округление результатов анализов (математические функции)
SELECT
    sample_number,
    ROUND(fe_content, 1) AS fe_rounded,
    CEIL(sio2_content) AS sio2_ceil,
    FLOOR(al2o3_content) AS al2o3_floor
FROM fact_ore_quality
WHERE date_id = 20240315
ORDER BY fe_content DESC;

-- Задание 2. Отклонение от целевого содержания Fe (ABS, SIGN, POWER)
SELECT
    sample_number,
    fe_content,
    ROUND(fe_content - 60, 2) AS deviation,
    ROUND(ABS(fe_content - 60), 2) AS abs_deviation,
    CASE
        WHEN SIGN(fe_content - 60) = 1 THEN 'Выше нормы'
        WHEN SIGN(fe_content - 60) = 0 THEN 'В норме'
        ELSE 'Ниже нормы'
    END AS direction,
    ROUND(POWER(fe_content - 60, 2), 2) AS squared_dev
FROM fact_ore_quality
WHERE date_id BETWEEN 20240301 AND 20240331
ORDER BY abs_deviation DESC
LIMIT 10;

-- Задание 3. Статистика добычи по сменам (агрегатные функции)
SELECT
    shift_id,
    CASE shift_id
        WHEN 1 THEN 'Утренняя'
        WHEN 2 THEN 'Дневная'
        WHEN 3 THEN 'Ночная'
        ELSE 'Неизвестная'
    END AS shift_name,
    COUNT(*) AS record_count,
    SUM(tons_mined) AS total_tons,
    ROUND(AVG(tons_mined), 2) AS avg_tons,
    COUNT(DISTINCT operator_id) AS unique_operators
FROM fact_production
WHERE date_id BETWEEN 20240301 AND 20240331
GROUP BY shift_id
ORDER BY shift_id;

-- Задание 4. Список причин простоев по оборудованию (STRING_AGG)
SELECT
    e.equipment_name,
    STRING_AGG(DISTINCT dr.reason_name, '; ' ORDER BY dr.reason_name) AS reasons,
    SUM(fd.duration_min) AS total_min,
    COUNT(*) AS incidents
FROM fact_equipment_downtime fd
JOIN dim_equipment e ON fd.equipment_id = e.equipment_id
JOIN dim_downtime_reason dr ON fd.reason_id = dr.reason_id
WHERE fd.date_id BETWEEN 20240301 AND 20240331
GROUP BY e.equipment_name
ORDER BY total_min DESC;

-- Задание 5. Преобразование date_id и форматирование отчёта (CAST, TO_CHAR)
SELECT
    date_id,
    TO_CHAR(TO_DATE(date_id::VARCHAR, 'YYYYMMDD'), 'DD.MM.YYYY') AS formatted_date,
    SUM(tons_mined) AS total_tons,
    TO_CHAR(SUM(tons_mined), 'FM999G999D00') AS formatted_tons
FROM fact_production
WHERE date_id BETWEEN 20240301 AND 20240307
GROUP BY date_id
ORDER BY date_id;

-- Задание 6. Классификация проб и расчёт процента качества (CASE, COALESCE, NULLIF)
SELECT
    d.full_date,
    COUNT(CASE WHEN fq.fe_content >= 65 THEN 1 END) AS rich_ore_count,
    COUNT(CASE WHEN fq.fe_content >= 55 AND fq.fe_content < 65 THEN 1 END) AS medium_ore_count,
    COUNT(CASE WHEN fq.fe_content < 55 THEN 1 END) AS poor_ore_count,
    COUNT(*) AS total_samples,
    ROUND(
        COUNT(CASE WHEN fq.fe_content >= 60 THEN 1 END) * 100.0 /
        NULLIF(COUNT(*), 0),
        2
    ) AS good_quality_percent
FROM fact_ore_quality fq
JOIN dim_date d ON fq.date_id = d.date_id
WHERE fq.date_id BETWEEN 20240301 AND 20240331
GROUP BY d.full_date
ORDER BY d.full_date;

-- Задание 7. Безопасные KPI с обработкой NULL и нуля (COALESCE, NULLIF, GREATEST)
SELECT
    e.equipment_name,
    COALESCE(SUM(fp.tons_mined), 0) AS total_tons,
    COALESCE(ROUND(AVG(fp.tons_mined), 2), 0) AS avg_tons_per_shift,
    COALESCE(SUM(fp.fuel_consumed_l), 0) AS total_fuel,
    ROUND(
        COALESCE(SUM(fp.tons_mined), 0) /
        NULLIF(GREATEST(SUM(fp.fuel_consumed_l), 0.1), 0),
        2
    ) AS tons_per_liter,
    CASE
        WHEN COALESCE(SUM(fp.tons_mined), 0) > 1000 THEN 'Высокая'
        WHEN COALESCE(SUM(fp.tons_mined), 0) > 500 THEN 'Средняя'
        ELSE 'Низкая'
    END AS productivity_category
FROM dim_equipment e
LEFT JOIN fact_production fp ON e.equipment_id = fp.equipment_id
    AND fp.date_id BETWEEN 20240301 AND 20240331
GROUP BY e.equipment_id, e.equipment_name
ORDER BY total_tons DESC;

-- Задание 8. Анализ пропусков данных (IS NULL, COUNT, CASE)
SELECT
    'fact_ore_quality' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(fe_content) AS fe_not_null,
    COUNT(*) - COUNT(fe_content) AS fe_null_count,
    ROUND((COUNT(*) - COUNT(fe_content)) * 100.0 / NULLIF(COUNT(*), 0), 2) AS fe_null_percent,
    COUNT(sio2_content) AS sio2_not_null,
    COUNT(al2o3_content) AS al2o3_not_null,
    CASE
        WHEN COUNT(fe_content) = COUNT(*) THEN 'Полные данные'
        WHEN COUNT(fe_content) >= COUNT(*) * 0.9 THEN 'Минимальные пропуски'
        ELSE 'Значительные пропуски'
    END AS data_quality
FROM fact_ore_quality
WHERE date_id BETWEEN 20240301 AND 20240331

UNION ALL

SELECT
    'fact_production' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(tons_mined) AS tons_not_null,
    COUNT(*) - COUNT(tons_mined) AS tons_null_count,
    ROUND((COUNT(*) - COUNT(tons_mined)) * 100.0 / NULLIF(COUNT(*), 0), 2) AS tons_null_percent,
    COUNT(fuel_consumed_l) AS fuel_not_null,
    COUNT(operator_id) AS operator_not_null,
    CASE
        WHEN COUNT(tons_mined) = COUNT(*) THEN 'Полные данные'
        WHEN COUNT(tons_mined) >= COUNT(*) * 0.95 THEN 'Минимальные пропуски'
        ELSE 'Значительные пропуски'
    END AS data_quality
FROM fact_production
WHERE date_id BETWEEN 20240301 AND 20240331;

-- Задание 9. Комплексный отчёт по эффективности оборудования
SELECT
    e.equipment_name,
    m.mine_name,
    et.type_name,
    COUNT(DISTINCT fp.date_id) AS working_days,
    COALESCE(SUM(fp.tons_mined), 0) AS total_tons_mined,
    COALESCE(SUM(fp.operating_hours), 0) AS total_hours,
    ROUND(
        COALESCE(SUM(fp.tons_mined), 0) /
        NULLIF(COALESCE(SUM(fp.operating_hours), 1), 0),
        2
    ) AS tons_per_hour,
    COALESCE(SUM(fp.fuel_consumed_l), 0) AS total_fuel,
    ROUND(
        COALESCE(SUM(fp.tons_mined), 0) /
        NULLIF(GREATEST(COALESCE(SUM(fp.fuel_consumed_l), 0), 0.1), 0),
        2
    ) AS tons_per_liter,
    COUNT(DISTINCT fp.operator_id) AS operators_count,
    COALESCE(SUM(fd.duration_min), 0) AS total_downtime_min,
    ROUND(
        COALESCE(SUM(fd.duration_min), 0) * 100.0 /
        NULLIF(COALESCE(SUM(fp.operating_hours) * 60, 1), 0),
        2
    ) AS downtime_percent
FROM dim_equipment e
JOIN dim_mine m ON e.mine_id = m.mine_id
JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
LEFT JOIN fact_production fp ON e.equipment_id = fp.equipment_id
    AND fp.date_id BETWEEN 20240301 AND 20240331
LEFT JOIN fact_equipment_downtime fd ON e.equipment_id = fd.equipment_id
    AND fd.date_id BETWEEN 20240301 AND 20240331
GROUP BY e.equipment_id, e.equipment_name, m.mine_name, et.type_name
ORDER BY tons_per_hour DESC;

-- Задание 10. Категоризация простоев (все функции модуля)
SELECT
    TO_CHAR(d.full_date, 'DD.MM.YYYY') AS formatted_date,
    ds.shift_name,
    e.equipment_name,
    dr.reason_name,
    dr.category,
    fd.duration_min,
    ROUND(fd.duration_min / 60.0, 2) AS duration_hours,
    CASE
        WHEN fd.duration_min >= 480 THEN 'Критический (>8ч)'
        WHEN fd.duration_min >= 240 THEN 'Значительный (4-8ч)'
        WHEN fd.duration_min >= 60 THEN 'Средний (1-4ч)'
        ELSE 'Короткий (<1ч)'
    END AS duration_category,
    CASE
        WHEN dr.category = 'плановый' THEN 'Плановые работы'
        WHEN dr.category = 'внеплановый' AND fd.duration_min > 120 THEN 'Аварийные'
        WHEN dr.category = 'внеплановый' THEN 'Внеплановые'
        ELSE 'Прочие'
    END AS downtime_type,
    o.last_name || ' ' || LEFT(o.first_name, 1) || '.' AS operator_short,
    COUNT(*) OVER (PARTITION BY e.equipment_id) AS total_incidents_for_equipment,
    SUM(fd.duration_min) OVER (PARTITION BY e.equipment_id) AS total_downtime_for_equipment
FROM fact_equipment_downtime fd
JOIN dim_date d ON fd.date_id = d.date_id
JOIN dim_shift ds ON fd.shift_id = ds.shift_id
JOIN dim_equipment e ON fd.equipment_id = e.equipment_id
JOIN dim_downtime_reason dr ON fd.reason_id = dr.reason_id
LEFT JOIN dim_operator o ON fd.operator_id = o.operator_id
WHERE fd.date_id BETWEEN 20240301 AND 20240331
ORDER BY d.full_date DESC, fd.duration_min DESC;

-- Дополнительное задание Б1. Расчёт RMSE содержания Fe
SELECT
    ROUND(
        SQRT(
            AVG(
                POWER(fe_content - 60, 2)
            )
        ),
        3
    ) AS rmse_fe_content
FROM fact_ore_quality
WHERE date_id BETWEEN 20240301 AND 20240331;

-- Дополнительное задание Б2. Условная агрегация с FILTER
SELECT
    e.equipment_name,
    COUNT(*) AS total_shifts,
    COUNT(*) FILTER (WHERE fp.tons_mined > 100) AS high_productivity_shifts,
    COUNT(*) FILTER (WHERE fp.tons_mined BETWEEN 50 AND 100) AS medium_productivity_shifts,
    COUNT(*) FILTER (WHERE fp.tons_mined < 50) AS low_productivity_shifts,
    ROUND(
        COUNT(*) FILTER (WHERE fp.tons_mined > 100) * 100.0 /
        NULLIF(COUNT(*), 0),
        2
    ) AS high_productivity_percent
FROM dim_equipment e
LEFT JOIN fact_production fp ON e.equipment_id = fp.equipment_id
    AND fp.date_id BETWEEN 20240301 AND 20240331
GROUP BY e.equipment_id, e.equipment_name
ORDER BY high_productivity_percent DESC NULLS LAST;
