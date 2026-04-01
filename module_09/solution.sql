-- Модуль 9: Колоночное хранение и оптимизация аналитических запросов

-- Задание 1. Колоночная таблица фактов добычи (Citus Columnar)
CREATE TABLE fact_production_columnar (
    production_id BIGINT,
    date_id INTEGER,
    shift_id INTEGER,
    mine_id INTEGER,
    shaft_id INTEGER,
    equipment_id INTEGER,
    operator_id INTEGER,
    location_id INTEGER,
    ore_grade_id INTEGER,
    tons_mined NUMERIC,
    tons_transported NUMERIC,
    trips_count INTEGER,
    distance_km NUMERIC,
    fuel_consumed_l NUMERIC,
    operating_hours NUMERIC,
    loaded_at TIMESTAMP
) USING columnar;

INSERT INTO fact_production_columnar
SELECT * FROM fact_production;

SELECT 'row_store' AS storage,
       pg_size_pretty(pg_total_relation_size('fact_production')) AS size
UNION ALL
SELECT 'column_store',
       pg_size_pretty(pg_total_relation_size('fact_production_columnar'));

-- Задание 2. BRIN-индекс для таблицы простоев
CREATE INDEX idx_downtime_date_brin
    ON fact_equipment_downtime
    USING brin (date_id)
    WITH (pages_per_range = 32);

CREATE INDEX idx_downtime_date_btree
    ON fact_equipment_downtime (date_id);

SELECT indexrelname,
       pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes
WHERE relname = 'fact_equipment_downtime'
  AND indexrelname IN ('idx_downtime_date_brin', 'idx_downtime_date_btree');

EXPLAIN (ANALYZE, BUFFERS)
SELECT d.equipment_id,
       r.reason_name,
       SUM(d.duration_min) AS total_downtime
FROM fact_equipment_downtime d
JOIN dim_downtime_reason r ON d.reason_id = r.reason_id
WHERE d.date_id BETWEEN 20240201 AND 20240228
GROUP BY d.equipment_id, r.reason_name
ORDER BY total_downtime DESC;

DROP INDEX IF EXISTS idx_downtime_date_brin, idx_downtime_date_btree;

-- Задание 3. Секционирование таблицы качества руды
CREATE TABLE fact_ore_quality_partitioned (
    quality_id BIGINT,
    date_id INTEGER NOT NULL,
    mine_id INTEGER NOT NULL,
    location_id INTEGER,
    equipment_id INTEGER,
    ore_grade_id INTEGER,
    fe_content NUMERIC(5,2),
    moisture_pct NUMERIC(5,2),
    density NUMERIC(6,3),
    sample_weight_kg NUMERIC(8,2),
    shift_id INTEGER
) PARTITION BY LIST (mine_id);

CREATE TABLE fact_ore_quality_mine_1 PARTITION OF fact_ore_quality_partitioned
    FOR VALUES IN (1);

CREATE TABLE fact_ore_quality_mine_2 PARTITION OF fact_ore_quality_partitioned
    FOR VALUES IN (2);

INSERT INTO fact_ore_quality_partitioned
SELECT * FROM fact_ore_quality;

SELECT tableoid::regclass AS partition_name,
       COUNT(*) AS row_count
FROM fact_ore_quality_partitioned
GROUP BY tableoid;

EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM fact_ore_quality_partitioned
WHERE mine_id = 1;

-- Задание 4. Комбинация секционирования и BRIN
CREATE TABLE fact_telemetry_optimized (
    telemetry_id BIGINT,
    date_id INTEGER NOT NULL,
    time_id INTEGER,
    equipment_id INTEGER,
    sensor_id INTEGER,
    location_id INTEGER,
    sensor_value NUMERIC,
    is_alarm BOOLEAN,
    quality_flag VARCHAR(10),
    loaded_at TIMESTAMP
) PARTITION BY RANGE (date_id);

CREATE TABLE fact_telemetry_2024_01 PARTITION OF fact_telemetry_optimized
    FOR VALUES FROM (20240101) TO (20240201);

CREATE TABLE fact_telemetry_2024_02 PARTITION OF fact_telemetry_optimized
    FOR VALUES FROM (20240201) TO (20240301);

CREATE TABLE fact_telemetry_2024_03 PARTITION OF fact_telemetry_optimized
    FOR VALUES FROM (20240301) TO (20240401);

CREATE TABLE fact_telemetry_2024_04 PARTITION OF fact_telemetry_optimized
    FOR VALUES FROM (20240401) TO (20240501);

CREATE TABLE fact_telemetry_2024_05 PARTITION OF fact_telemetry_optimized
    FOR VALUES FROM (20240501) TO (20240601);

CREATE TABLE fact_telemetry_2024_06 PARTITION OF fact_telemetry_optimized
    FOR VALUES FROM (20240601) TO (20240701);

INSERT INTO fact_telemetry_optimized
SELECT telemetry_id, date_id, time_id, equipment_id, sensor_id, location_id,
       sensor_value, is_alarm, quality_flag, loaded_at
FROM fact_equipment_telemetry;

CREATE INDEX idx_tel_opt_2024_01_brin ON fact_telemetry_2024_01 USING brin (date_id);
CREATE INDEX idx_tel_opt_2024_02_brin ON fact_telemetry_2024_02 USING brin (date_id);
CREATE INDEX idx_tel_opt_2024_03_brin ON fact_telemetry_2024_03 USING brin (date_id);
CREATE INDEX idx_tel_opt_2024_04_brin ON fact_telemetry_2024_04 USING brin (date_id);
CREATE INDEX idx_tel_opt_2024_05_brin ON fact_telemetry_2024_05 USING brin (date_id);
CREATE INDEX idx_tel_opt_2024_06_brin ON fact_telemetry_2024_06 USING brin (date_id);

EXPLAIN (ANALYZE, BUFFERS)
SELECT equipment_id,
       AVG(sensor_value) AS avg_value,
       MIN(sensor_value) AS min_value,
       MAX(sensor_value) AS max_value
FROM fact_telemetry_optimized
WHERE date_id BETWEEN 20240215 AND 20240315
GROUP BY equipment_id
ORDER BY avg_value DESC;

-- Задание 5. Анализ VertiPaq в DAX Studio
-- DAX запросы сохраняются в отдельный файл lab_solutions.dax
-- Этот файл содержит только SQL запросы

-- Задание 6. Сравнение производительности запросов
EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT equipment_id,
       AVG(sensor_value) AS avg_temp
FROM fact_equipment_telemetry
WHERE date_id BETWEEN 20240101 AND 20240331
  AND sensor_id IN (SELECT sensor_id FROM dim_sensor WHERE sensor_type_id = 1)
GROUP BY equipment_id;

CREATE TABLE fact_telemetry_columnar (
    telemetry_id BIGINT,
    date_id INTEGER,
    time_id INTEGER,
    equipment_id INTEGER,
    sensor_id INTEGER,
    location_id INTEGER,
    sensor_value NUMERIC,
    is_alarm BOOLEAN,
    quality_flag VARCHAR(10),
    loaded_at TIMESTAMP
) USING columnar;

INSERT INTO fact_telemetry_columnar
SELECT * FROM fact_equipment_telemetry;

EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT equipment_id,
       AVG(sensor_value) AS avg_temp
FROM fact_telemetry_columnar
WHERE date_id BETWEEN 20240101 AND 20240331
  AND sensor_id IN (SELECT sensor_id FROM dim_sensor WHERE sensor_type_id = 1)
GROUP BY equipment_id;

EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT equipment_id,
       AVG(sensor_value) AS avg_temp
FROM fact_telemetry_optimized
WHERE date_id BETWEEN 20240101 AND 20240331
  AND sensor_id IN (SELECT sensor_id FROM dim_sensor WHERE sensor_type_id = 1)
GROUP BY equipment_id;

-- Задание 7. Проектирование стратегии хранения
-- Горячие данные (последние 3 месяца): heap + B-tree
-- Тёплые данные (3-12 месяцев): heap + BRIN
-- Холодные данные (> 1 года): columnar (сжатие, только чтение)

CREATE TABLE fact_telemetry_strategic (
    telemetry_id BIGINT,
    date_id INTEGER NOT NULL,
    time_id INTEGER,
    equipment_id INTEGER,
    sensor_id INTEGER,
    location_id INTEGER,
    sensor_value NUMERIC,
    is_alarm BOOLEAN,
    quality_flag VARCHAR(10),
    loaded_at TIMESTAMP,
    data_category VARCHAR(10) DEFAULT 'hot'
) PARTITION BY LIST (data_category);

-- Горячие данные (heap + B-tree)
CREATE TABLE fact_telemetry_hot PARTITION OF fact_telemetry_strategic
    FOR VALUES IN ('hot');

CREATE INDEX idx_telemetry_hot_date ON fact_telemetry_hot (date_id);
CREATE INDEX idx_telemetry_hot_equipment ON fact_telemetry_hot (equipment_id);
CREATE INDEX idx_telemetry_hot_sensor ON fact_telemetry_hot (sensor_id);

-- Тёплые данные (heap + BRIN)
CREATE TABLE fact_telemetry_warm PARTITION OF fact_telemetry_strategic
    FOR VALUES IN ('warm');

CREATE INDEX idx_telemetry_warm_date_brin ON fact_telemetry_warm USING brin (date_id);
CREATE INDEX idx_telemetry_warm_equipment_brin ON fact_telemetry_warm USING brin (equipment_id);

-- Холодные данные (columnar)
CREATE TABLE fact_telemetry_cold PARTITION OF fact_telemetry_strategic
    FOR VALUES IN ('cold');

-- Очистка созданных таблиц и индексов
DROP TABLE IF EXISTS fact_production_columnar;
DROP TABLE IF EXISTS fact_ore_quality_partitioned;
DROP TABLE IF EXISTS fact_telemetry_optimized;
DROP TABLE IF EXISTS fact_telemetry_columnar;
DROP TABLE IF EXISTS fact_telemetry_strategic;
