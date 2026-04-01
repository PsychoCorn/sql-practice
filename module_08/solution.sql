-- Модуль 8: Проектирование стратегий оптимизированных индексов

-- Задание 1. Анализ селективности
SELECT attname AS column_name,
       n_distinct,
       correlation,
       null_frac
FROM pg_stats
WHERE tablename = 'fact_production'
  AND schemaname = 'public'
ORDER BY attname;

-- Задание 2. Покрывающий индекс для отчёта по добыче
EXPLAIN (ANALYZE, BUFFERS)
SELECT date_id,
       SUM(tons_mined) AS total_tons,
       SUM(trips_count) AS total_trips,
       SUM(operating_hours) AS total_hours
FROM fact_production
WHERE equipment_id = 5
  AND date_id BETWEEN 20240101 AND 20240331
GROUP BY date_id
ORDER BY date_id;

CREATE INDEX IF NOT EXISTS idx_fact_production_equipment_date_include
ON fact_production(equipment_id, date_id)
INCLUDE (tons_mined, trips_count, operating_hours);

EXPLAIN (ANALYZE, BUFFERS)
SELECT date_id,
       SUM(tons_mined) AS total_tons,
       SUM(trips_count) AS total_trips,
       SUM(operating_hours) AS total_hours
FROM fact_production
WHERE equipment_id = 5
  AND date_id BETWEEN 20240101 AND 20240331
GROUP BY date_id
ORDER BY date_id;

-- Задание 3. Частичный индекс для тревожных показаний
EXPLAIN (ANALYZE, BUFFERS)
SELECT t.date_id, t.time_id,
       s.sensor_code,
       t.sensor_value,
       t.quality_flag
FROM fact_equipment_telemetry t
JOIN dim_sensor s ON s.sensor_id = t.sensor_id
WHERE t.equipment_id = 7
  AND t.is_alarm = TRUE
  AND t.date_id = 20240315
ORDER BY t.time_id DESC;

CREATE INDEX IF NOT EXISTS idx_fact_telemetry_alarm_equipment_date
ON fact_equipment_telemetry(equipment_id, date_id, time_id DESC)
WHERE is_alarm = TRUE;

EXPLAIN (ANALYZE, BUFFERS)
SELECT t.date_id, t.time_id,
       s.sensor_code,
       t.sensor_value,
       t.quality_flag
FROM fact_equipment_telemetry t
JOIN dim_sensor s ON s.sensor_id = t.sensor_id
WHERE t.equipment_id = 7
  AND t.is_alarm = TRUE
  AND t.date_id = 20240315
ORDER BY t.time_id DESC;

SELECT indexrelname,
       pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes
WHERE relname = 'fact_equipment_telemetry'
  AND indexrelname IN ('idx_fact_telemetry_alarm_equipment_date', 'idx_fact_telemetry_date');

-- Задание 4. Индекс на выражении
EXPLAIN (ANALYZE, BUFFERS)
SELECT fd.downtime_id, fd.date_id,
       e.equipment_name,
       dr.reason_name,
       fd.duration_min,
       ROUND(fd.duration_min / 60.0, 1) AS duration_hours
FROM fact_equipment_downtime fd
JOIN dim_equipment e ON e.equipment_id = fd.equipment_id
JOIN dim_downtime_reason dr ON dr.reason_id = fd.reason_id
WHERE fd.duration_min / 60.0 > 4
ORDER BY fd.duration_min DESC;

CREATE INDEX IF NOT EXISTS idx_fact_downtime_duration_hours
ON fact_equipment_downtime((duration_min / 60.0));

EXPLAIN (ANALYZE, BUFFERS)
SELECT fd.downtime_id, fd.date_id,
       e.equipment_name,
       dr.reason_name,
       fd.duration_min,
       ROUND(fd.duration_min / 60.0, 1) AS duration_hours
FROM fact_equipment_downtime fd
JOIN dim_equipment e ON e.equipment_id = fd.equipment_id
JOIN dim_downtime_reason dr ON dr.reason_id = fd.reason_id
WHERE fd.duration_min / 60.0 > 4
ORDER BY fd.duration_min DESC;

EXPLAIN (ANALYZE, BUFFERS)
SELECT fd.downtime_id, fd.date_id,
       e.equipment_name,
       dr.reason_name,
       fd.duration_min,
       ROUND(fd.duration_min / 60.0, 1) AS duration_hours
FROM fact_equipment_downtime fd
JOIN dim_equipment e ON e.equipment_id = fd.equipment_id
JOIN dim_downtime_reason dr ON dr.reason_id = fd.reason_id
WHERE fd.duration_min > 240
ORDER BY fd.duration_min DESC;

-- Задание 5. Составной индекс: порядок столбцов
CREATE INDEX IF NOT EXISTS idx_test_a ON fact_production(mine_id, date_id, shift_id);
CREATE INDEX IF NOT EXISTS idx_test_b ON fact_production(date_id, mine_id, shift_id);
CREATE INDEX IF NOT EXISTS idx_test_c ON fact_production(shift_id, mine_id, date_id);

DROP INDEX IF EXISTS idx_test_b, idx_test_c;
EXPLAIN (ANALYZE, BUFFERS)
SELECT p.date_id, p.shift_id,
       SUM(p.tons_mined) AS total_tons,
       AVG(p.fuel_consumed_l) AS avg_fuel
FROM fact_production p
WHERE p.mine_id = 1
  AND p.date_id BETWEEN 20240201 AND 20240229
GROUP BY p.date_id, p.shift_id
ORDER BY p.date_id, p.shift_id;

DROP INDEX IF EXISTS idx_test_a;
CREATE INDEX IF NOT EXISTS idx_test_b ON fact_production(date_id, mine_id, shift_id);
EXPLAIN (ANALYZE, BUFFERS)
SELECT p.date_id, p.shift_id,
       SUM(p.tons_mined) AS total_tons,
       AVG(p.fuel_consumed_l) AS avg_fuel
FROM fact_production p
WHERE p.mine_id = 1
  AND p.date_id BETWEEN 20240201 AND 20240229
GROUP BY p.date_id, p.shift_id
ORDER BY p.date_id, p.shift_id;

DROP INDEX IF EXISTS idx_test_b;
CREATE INDEX IF NOT EXISTS idx_test_c ON fact_production(shift_id, mine_id, date_id);
EXPLAIN (ANALYZE, BUFFERS)
SELECT p.date_id, p.shift_id,
       SUM(p.tons_mined) AS total_tons,
       AVG(p.fuel_consumed_l) AS avg_fuel
FROM fact_production p
WHERE p.mine_id = 1
  AND p.date_id BETWEEN 20240201 AND 20240229
GROUP BY p.date_id, p.shift_id
ORDER BY p.date_id, p.shift_id;

DROP INDEX IF EXISTS idx_test_a, idx_test_b, idx_test_c;

-- Задание 6. BRIN-индекс для телеметрии
SELECT correlation
FROM pg_stats
WHERE tablename = 'fact_equipment_telemetry' AND attname = 'date_id';

CREATE INDEX IF NOT EXISTS idx_telemetry_date_brin
ON fact_equipment_telemetry USING BRIN (date_id)
WITH (pages_per_range = 64);

CREATE INDEX IF NOT EXISTS idx_telemetry_date_btree
ON fact_equipment_telemetry(date_id);

SELECT indexrelname,
       pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes
WHERE relname = 'fact_equipment_telemetry'
  AND indexrelname IN ('idx_telemetry_date_brin', 'idx_telemetry_date_btree');

EXPLAIN (ANALYZE, BUFFERS)
SELECT t.time_id, t.sensor_id, t.sensor_value, t.is_alarm
FROM fact_equipment_telemetry t
WHERE t.date_id = 20240315;

DROP INDEX IF EXISTS idx_telemetry_date_brin, idx_telemetry_date_btree;

-- Задание 7. Мониторинг и очистка индексов
SELECT
    relname AS table_name,
    COUNT(*) AS index_count,
    pg_size_pretty(SUM(pg_relation_size(indexrelid))) AS total_index_size,
    pg_size_pretty(pg_relation_size(relid)) AS table_size,
    ROUND(SUM(pg_relation_size(indexrelid)) * 100.0 / pg_relation_size(relid), 2) AS index_to_table_ratio
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
GROUP BY relname, relid
ORDER BY SUM(pg_relation_size(indexrelid)) DESC;

SELECT
    a.indexrelid::regclass AS index1,
    b.indexrelid::regclass AS index2,
    a.indrelid::regclass AS table_name
FROM pg_index a
JOIN pg_index b ON a.indrelid = b.indrelid
    AND a.indexrelid < b.indexrelid
    AND a.indkey = b.indkey
WHERE a.indrelid::regclass::text NOT LIKE 'pg_%';

SELECT
    schemaname,
    relname AS table_name,
    indexrelname AS index_name,
    pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
    idx_scan AS times_used
FROM pg_stat_user_indexes
WHERE idx_scan = 0
  AND schemaname = 'public'
ORDER BY pg_relation_size(indexrelid) DESC;

-- Задание 8. Комплексная оптимизация
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH production_data AS (
    SELECT
        p.equipment_id,
        SUM(p.operating_hours) AS total_operating_hours,
        SUM(p.tons_mined) AS total_tons
    FROM fact_production p
    WHERE p.date_id BETWEEN 20240301 AND 20240331
    GROUP BY p.equipment_id
),
downtime_data AS (
    SELECT
        fd.equipment_id,
        SUM(fd.duration_min) / 60.0 AS total_downtime_hours,
        SUM(CASE WHEN fd.is_planned = FALSE THEN fd.duration_min ELSE 0 END) / 60.0 AS unplanned_hours
    FROM fact_equipment_downtime fd
    WHERE fd.date_id BETWEEN 20240301 AND 20240331
    GROUP BY fd.equipment_id
)
SELECT
    e.equipment_name,
    et.type_name,
    COALESCE(pd.total_operating_hours, 0) AS operating_hours,
    COALESCE(dd.total_downtime_hours, 0) AS downtime_hours,
    COALESCE(dd.unplanned_downtime, 0) AS unplanned_downtime,
    COALESCE(pd.total_tons, 0) AS tons_mined,
    CASE
        WHEN COALESCE(pd.total_operating_hours, 0) + COALESCE(dd.total_downtime_hours, 0) > 0
        THEN ROUND(
            COALESCE(pd.total_operating_hours, 0) /
            (COALESCE(pd.total_operating_hours, 0) + COALESCE(dd.total_downtime_hours, 0)) * 100, 1
        )
        ELSE 0
    END AS availability_pct
FROM dim_equipment e
JOIN dim_equipment_type et ON et.equipment_type_id = e.equipment_type_id
LEFT JOIN production_data pd ON pd.equipment_id = e.equipment_id
LEFT JOIN downtime_data dd ON dd.equipment_id = e.equipment_id
WHERE e.status = 'active'
ORDER BY availability_pct ASC;

CREATE INDEX IF NOT EXISTS idx_fact_production_date_equipment_include
ON fact_production(date_id, equipment_id) INCLUDE (operating_hours, tons_mined);

CREATE INDEX IF NOT EXISTS idx_fact_downtime_date_equipment_include
ON fact_equipment_downtime(date_id, equipment_id) INCLUDE (duration_min, is_planned);

CREATE INDEX IF NOT EXISTS idx_dim_equipment_status
ON dim_equipment(status) WHERE status = 'active';

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH production_data AS (
    SELECT
        p.equipment_id,
        SUM(p.operating_hours) AS total_operating_hours,
        SUM(p.tons_mined) AS total_tons
    FROM fact_production p
    WHERE p.date_id BETWEEN 20240301 AND 20240331
    GROUP BY p.equipment_id
),
downtime_data AS (
    SELECT
        fd.equipment_id,
        SUM(fd.duration_min) / 60.0 AS total_downtime_hours,
        SUM(CASE WHEN fd.is_planned = FALSE THEN fd.duration_min ELSE 0 END) / 60.0 AS unplanned_hours
    FROM fact_equipment_downtime fd
    WHERE fd.date_id BETWEEN 20240301 AND 20240331
    GROUP BY fd.equipment_id
)
SELECT
    e.equipment_name,
    et.type_name,
    COALESCE(pd.total_operating_hours, 0) AS operating_hours,
    COALESCE(dd.total_downtime_hours, 0) AS downtime_hours,
    COALESCE(dd.unplanned_downtime, 0) AS unplanned_downtime,
    COALESCE(pd.total_tons, 0) AS tons_mined,
    CASE
        WHEN COALESCE(pd.total_operating_hours, 0) + COALESCE(dd.total_downtime_hours, 0) > 0
        THEN ROUND(
            COALESCE(pd.total_operating_hours, 0) /
            (COALESCE(pd.total_operating_hours, 0) + COALESCE(dd.total_downtime_hours, 0)) * 100, 1
        )
        ELSE 0
    END AS availability_pct
FROM dim_equipment e
JOIN dim_equipment_type et ON et.equipment_type_id = e.equipment_type_id
LEFT JOIN production_data pd ON pd.equipment_id = e.equipment_id
LEFT JOIN downtime_data dd ON dd.equipment_id = e.equipment_id
WHERE e.status = 'active'
ORDER BY availability_pct ASC;

DROP INDEX IF EXISTS idx_fact_production_date_equipment_include;
DROP INDEX IF EXISTS idx_fact_downtime_date_equipment_include;
DROP INDEX IF EXISTS idx_dim_equipment_status;

-- Задание 9. CREATE INDEX CONCURRENTLY
\timing on
CREATE INDEX IF NOT EXISTS idx_telemetry_sensor_regular
    ON fact_equipment_telemetry(sensor_id, date_id);
\timing off

DROP INDEX IF EXISTS idx_telemetry_sensor_regular;

\timing on
CREATE INDEX CONCURRENTLY idx_telemetry_sensor_concurrent
    ON fact_equipment_telemetry(sensor_id, date_id);
\timing off

SELECT indexrelid::regclass, indisvalid
FROM pg_index
WHERE indexrelid = 'idx_telemetry_sensor_concurrent'::regclass;

DROP INDEX IF EXISTS idx_telemetry_sensor_concurrent;

-- Задание 10. Расширенная статистика
EXPLAIN ANALYZE
SELECT *
FROM fact_production
WHERE mine_id = 1
  AND shaft_id = 1
  AND date_id BETWEEN 20240101 AND 20240131;

CREATE STATISTICS IF NOT EXISTS stat_prod_mine_shaft_date (dependencies)
    ON mine_id, shaft_id, date_id FROM fact_production;

ANALYZE fact_production;

EXPLAIN ANALYZE
SELECT *
FROM fact_production
WHERE mine_id = 1
  AND shaft_id = 1
  AND date_id BETWEEN 20240101 AND 20240131;

SELECT stxname, stxkeys, stxkind
FROM pg_statistic_ext
WHERE stxname = 'stat_prod_mine_shaft_date';

DROP STATISTICS IF EXISTS stat_prod_mine_shaft_date;

-- Очистка всех созданных индексов
DROP INDEX IF EXISTS idx_fact_production_equipment_date_include;
DROP INDEX IF EXISTS idx_fact_telemetry_alarm_equipment_date;
DROP INDEX IF EXISTS idx_fact_downtime_duration_hours;
DROP INDEX IF EXISTS idx_test_a;
DROP INDEX IF EXISTS idx_test_b;
DROP INDEX IF EXISTS idx_test_c;
DROP INDEX IF EXISTS idx_telemetry_date_brin;
DROP INDEX IF EXISTS idx_telemetry_date_btree;
DROP INDEX IF EXISTS idx_fact_production_date_equipment_include;
DROP INDEX IF EXISTS idx_fact_downtime_date_equipment_include;
DROP INDEX IF EXISTS idx_dim_equipment_status;
DROP INDEX IF EXISTS idx_telemetry_sensor_regular;
DROP INDEX IF EXISTS idx_telemetry_sensor_concurrent;
