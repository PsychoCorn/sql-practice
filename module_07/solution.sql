
-- Задание 1. Анализ существующих индексов


-- 1.1 Список всех индексов факт-таблиц
EXPLAIN ANALYZE
SELECT tablename, indexname, indexdef
FROM pg_indexes
WHERE tablename IN ('fact_production', 'fact_equipment_telemetry',
                    'fact_equipment_downtime', 'fact_ore_quality')
  AND schemaname = 'public'
ORDER BY tablename, indexname;

-- 1.2 Размеры и использование индексов fact_production
SELECT indexrelname AS index_name,
       pg_size_pretty(pg_relation_size(indexrelid)) AS index_size,
       idx_scan AS times_used
FROM pg_stat_user_indexes
WHERE relname = 'fact_production'
  AND schemaname = 'public'
ORDER BY pg_relation_size(indexrelid) DESC;

-- 1.3 Суммарный размер индексов по факт-таблицам
SELECT relname AS tablename,
       COUNT(*) AS index_count,
       pg_size_pretty(SUM(pg_relation_size(indexrelid))) AS total_index_size
FROM pg_stat_user_indexes
WHERE relname IN ('fact_production', 'fact_equipment_telemetry',
                    'fact_equipment_downtime', 'fact_ore_quality')
  AND schemaname = 'public'
GROUP BY relname
ORDER BY SUM(pg_relation_size(indexrelid)) DESC;


-- Задание 2. Оптимизация поиска по расходу топлива


-- 2.1 План до индекса
EXPLAIN ANALYZE
SELECT p.date_id, e.equipment_name, o.last_name, p.fuel_consumed_l
FROM fact_production p
JOIN dim_equipment e ON p.equipment_id = e.equipment_id
JOIN dim_operator o ON p.operator_id = o.operator_id
WHERE p.fuel_consumed_l > 80
ORDER BY p.fuel_consumed_l DESC;

-- 2.2 Создание индекса
CREATE INDEX IF NOT EXISTS idx_fact_production_fuel_consumed
ON fact_production(fuel_consumed_l);

-- 2.3 План после индекса
EXPLAIN ANALYZE
SELECT p.date_id, e.equipment_name, o.last_name, p.fuel_consumed_l
FROM fact_production p
JOIN dim_equipment e ON p.equipment_id = e.equipment_id
JOIN dim_operator o ON p.operator_id = o.operator_id
WHERE p.fuel_consumed_l > 80
ORDER BY p.fuel_consumed_l DESC;


-- Задание 3. Частичный индекс для аварийной телеметрии


-- 3.1 План до индекса
EXPLAIN ANALYZE
SELECT t.telemetry_id, t.date_id, t.equipment_id,
       t.sensor_id, t.sensor_value
FROM fact_equipment_telemetry t
WHERE t.date_id = 20240315
  AND t.is_alarm = TRUE;

-- 3.2 Частичный индекс
CREATE INDEX IF NOT EXISTS idx_fact_telemetry_alarm_date
ON fact_equipment_telemetry(date_id)
WHERE is_alarm = TRUE;

-- 3.3 План после индекса
EXPLAIN ANALYZE
SELECT t.telemetry_id, t.date_id, t.equipment_id,
       t.sensor_id, t.sensor_value
FROM fact_equipment_telemetry t
WHERE t.date_id = 20240315
  AND t.is_alarm = TRUE;

-- 3.4 Сравнение размеров
SELECT indexrelname,
       pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes
WHERE relname = 'fact_equipment_telemetry'
  AND indexrelname IN ('idx_fact_telemetry_alarm_date', 'idx_fact_telemetry_date');


-- Задание 4. Композитный индекс для отчёта по добыче


-- 4.1 План до индекса
EXPLAIN ANALYZE
SELECT date_id, tons_mined, tons_transported,
       trips_count, operating_hours
FROM fact_production
WHERE equipment_id = 5
  AND date_id BETWEEN 20240301 AND 20240331;

-- 4.2 Индекс (equipment_id, date_id)
CREATE INDEX IF NOT EXISTS idx_fact_production_equipment_date
ON fact_production(equipment_id, date_id);

-- 4.3 Индекс (date_id, equipment_id) для сравнения
CREATE INDEX IF NOT EXISTS idx_fact_production_date_equipment
ON fact_production(date_id, equipment_id);

-- 4.4 План после индексов
EXPLAIN ANALYZE
SELECT date_id, tons_mined, tons_transported,
       trips_count, operating_hours
FROM fact_production
WHERE equipment_id = 5
  AND date_id BETWEEN 20240301 AND 20240331;

-- 4.5 Удаление менее эффективного индекса
DROP INDEX IF EXISTS idx_fact_production_date_equipment;


-- Задание 5. Индекс по выражению для поиска операторов


-- 5.1 План до индекса
EXPLAIN ANALYZE
SELECT operator_id, last_name, first_name,
       middle_name, position, qualification
FROM dim_operator
WHERE LOWER(last_name) = 'петров';

-- 5.2 Индекс по выражению
CREATE INDEX IF NOT EXISTS idx_dim_operator_lower_last_name
ON dim_operator(LOWER(last_name));

-- 5.3 План после индекса
EXPLAIN ANALYZE
SELECT operator_id, last_name, first_name,
       middle_name, position, qualification
FROM dim_operator
WHERE LOWER(last_name) = 'петров';

-- 5.4 Проверка без LOWER()
EXPLAIN ANALYZE
SELECT operator_id, last_name, first_name,
       middle_name, position, qualification
FROM dim_operator
WHERE last_name = 'Петров';


-- Задание 6. Покрывающий индекс для дашборда


-- 6.1 План до индекса
EXPLAIN ANALYZE
SELECT date_id, equipment_id, tons_mined
FROM fact_production
WHERE date_id = 20240315;

-- 6.2 Покрывающий индекс
CREATE INDEX IF NOT EXISTS idx_fact_production_date_equipment_include
ON fact_production(date_id) INCLUDE (equipment_id, tons_mined);

-- 6.3 План после индекса
EXPLAIN ANALYZE
SELECT date_id, equipment_id, tons_mined
FROM fact_production
WHERE date_id = 20240315;

-- 6.4 Проверка с дополнительным столбцом
EXPLAIN ANALYZE
SELECT date_id, equipment_id, tons_mined, fuel_consumed_l
FROM fact_production
WHERE date_id = 20240315;


-- Задание 7. BRIN-индекс для телеметрии


-- 7.1 Проверка B-tree индекса
SELECT indexrelname,
       pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes
WHERE indexrelname = 'idx_fact_telemetry_date';

-- 7.2 BRIN-индекс
CREATE INDEX IF NOT EXISTS idx_telemetry_date_brin
ON fact_equipment_telemetry USING brin (date_id)
WITH (pages_per_range = 128);

-- 7.3 Сравнение размеров
SELECT indexrelname,
       pg_size_pretty(pg_relation_size(indexrelid)) AS size,
       idx_scan AS times_used
FROM pg_stat_user_indexes
WHERE relname = 'fact_equipment_telemetry'
  AND indexrelname IN ('idx_fact_telemetry_date', 'idx_telemetry_date_brin')
  AND schemaname = 'public';

-- 7.4 Сравнение планов
EXPLAIN ANALYZE
SELECT * FROM fact_equipment_telemetry
WHERE date_id BETWEEN 20240301 AND 20240331;

SET enable_indexscan = off;
SET enable_bitmapscan = on;

EXPLAIN ANALYZE
SELECT * FROM fact_equipment_telemetry
WHERE date_id BETWEEN 20240301 AND 20240331;

RESET enable_indexscan;
RESET enable_bitmapscan;


-- Задание 8. Оптимизация запроса по простоям


-- 8.1 План до индекса
EXPLAIN ANALYZE
SELECT d.date_id, e.equipment_name,
       r.reason_name, r.category,
       dt.duration_min, dt.comment
FROM fact_equipment_downtime dt
JOIN dim_date d ON dt.date_id = d.date_id
JOIN dim_equipment e ON dt.equipment_id = e.equipment_id
JOIN dim_downtime_reason r ON dt.reason_id = r.reason_id
WHERE dt.equipment_id = 3
  AND dt.date_id BETWEEN 20240101 AND 20240331
  AND dt.is_planned = FALSE
ORDER BY dt.duration_min DESC;

-- 8.2 Частичный композитный индекс
CREATE INDEX IF NOT EXISTS idx_fact_downtime_equipment_date_unplanned
ON fact_equipment_downtime(equipment_id, date_id)
WHERE is_planned = FALSE;

-- 8.3 План после индекса
EXPLAIN ANALYZE
SELECT d.date_id, e.equipment_name,
       r.reason_name, r.category,
       dt.duration_min, dt.comment
FROM fact_equipment_downtime dt
JOIN dim_date d ON dt.date_id = d.date_id
JOIN dim_equipment e ON dt.equipment_id = e.equipment_id
JOIN dim_downtime_reason r ON dt.reason_id = r.reason_id
WHERE dt.equipment_id = 3
  AND dt.date_id BETWEEN 20240101 AND 20240331
  AND dt.is_planned = FALSE
ORDER BY dt.duration_min DESC;


-- Задание 9. Анализ влияния индексов на INSERT


-- 9.1 Количество индексов
SELECT COUNT(*) AS index_count
FROM pg_indexes
WHERE tablename = 'fact_production'
  AND schemaname = 'public';

-- 9.2 INSERT с текущими индексами
EXPLAIN ANALYZE
INSERT INTO fact_production
    (date_id, shift_id, mine_id, shaft_id, equipment_id,
     operator_id, location_id, ore_grade_id,
     tons_mined, tons_transported, trips_count,
     distance_km, fuel_consumed_l, operating_hours)
VALUES
    (20240401, 1, 1, 1, 1, 1, 1, 1,
     120.50, 115.00, 8, 12.5, 45.2, 7.5);

-- 9.3 Дополнительные индексы
CREATE INDEX IF NOT EXISTS idx_fact_production_mine_date
ON fact_production(mine_id, date_id);

CREATE INDEX IF NOT EXISTS idx_fact_production_operator_date
ON fact_production(operator_id, date_id);

CREATE INDEX IF NOT EXISTS idx_fact_production_tons_mined
ON fact_production(tons_mined);

-- 9.4 INSERT с дополнительными индексами
EXPLAIN ANALYZE
INSERT INTO fact_production
    (date_id, shift_id, mine_id, shaft_id, equipment_id,
     operator_id, location_id, ore_grade_id,
     tons_mined, tons_transported, trips_count,
     distance_km, fuel_consumed_l, operating_hours)
VALUES
    (20240402, 1, 1, 1, 1, 1, 1, 1,
     125.50, 120.00, 9, 13.0, 46.5, 7.8);

-- 9.5 Очистка
DELETE FROM fact_production
WHERE date_id IN (20240401, 20240402);

DROP INDEX IF EXISTS idx_fact_production_mine_date;
DROP INDEX IF EXISTS idx_fact_production_operator_date;
DROP INDEX IF EXISTS idx_fact_production_tons_mined;


-- Задание 10. Комплексная оптимизация


-- 10.1 Запрос 1: Суммарная добыча по шахте за месяц
EXPLAIN ANALYZE
SELECT m.mine_name,
       SUM(p.tons_mined) AS total_tons,
       SUM(p.operating_hours) AS total_hours
FROM fact_production p
JOIN dim_mine m ON p.mine_id = m.mine_id
WHERE p.date_id BETWEEN 20240301 AND 20240331
GROUP BY m.mine_name;

-- 10.2 Запрос 2: Среднее качество руды по сорту за квартал
EXPLAIN ANALYZE
SELECT g.grade_name,
       AVG(q.fe_content) AS avg_fe,
       AVG(q.sio2_content) AS avg_sio2,
       COUNT(*) AS samples
FROM fact_ore_quality q
JOIN dim_ore_grade g ON q.ore_grade_id = g.ore_grade_id
WHERE q.date_id BETWEEN 20240101 AND 20240331
GROUP BY g.grade_name;

-- 10.3 Запрос 3: Топ-5 оборудования по внеплановым простоям
EXPLAIN ANALYZE
SELECT e.equipment_name,
       SUM(dt.duration_min) AS total_downtime_min,
       COUNT(*) AS incidents
FROM fact_equipment_downtime dt
JOIN dim_equipment e ON dt.equipment_id = e.equipment_id
WHERE dt.is_planned = FALSE
  AND dt.date_id BETWEEN 20240301 AND 20240331
GROUP BY e.equipment_name
ORDER BY total_downtime_min DESC
LIMIT 5;

-- 10.4 Запрос 4: Последние аварийные показания
EXPLAIN ANALYZE
SELECT t.date_id, t.time_id, t.sensor_id,
       t.sensor_value, t.quality_flag
FROM fact_equipment_telemetry t
WHERE t.equipment_id = 5
  AND t.is_alarm = TRUE
ORDER BY t.date_id DESC, t.time_id DESC
LIMIT 20;

-- 10.5 Запрос 5: Добыча оператора за неделю
EXPLAIN ANALYZE
SELECT p.date_id, e.equipment_name,
       p.tons_mined, p.trips_count, p.operating_hours
FROM fact_production p
JOIN dim_equipment e ON p.equipment_id = e.equipment_id
WHERE p.operator_id = 3
  AND p.date_id BETWEEN 20240311 AND 20240317
ORDER BY p.date_id;

-- Создание индексов для оптимизации
CREATE INDEX IF NOT EXISTS idx_fact_production_date_mine
ON fact_production(date_id, mine_id) INCLUDE (tons_mined, operating_hours);

CREATE INDEX IF NOT EXISTS idx_fact_ore_quality_date_grade
ON fact_ore_quality(date_id, ore_grade_id) INCLUDE (fe_content, sio2_content);

CREATE INDEX IF NOT EXISTS idx_fact_downtime_unplanned_date_equipment
ON fact_equipment_downtime(date_id, equipment_id)
WHERE is_planned = FALSE;

CREATE INDEX IF NOT EXISTS idx_fact_telemetry_alarm_equipment_date
ON fact_equipment_telemetry(equipment_id, date_id DESC, time_id DESC)
WHERE is_alarm = TRUE;

CREATE INDEX IF NOT EXISTS idx_fact_production_operator_date_include
ON fact_production(operator_id, date_id) INCLUDE (equipment_id, tons_mined, trips_count, operating_hours);

-- Повторное выполнение запросов после создания индексов
-- 10.1
EXPLAIN ANALYZE
SELECT m.mine_name,
       SUM(p.tons_mined) AS total_tons,
       SUM(p.operating_hours) AS total_hours
FROM fact_production p
JOIN dim_mine m ON p.mine_id = m.mine_id
WHERE p.date_id BETWEEN 20240301 AND 20240331
GROUP BY m.mine_name;

-- 10.2
EXPLAIN ANALYZE
SELECT g.grade_name,
       AVG(q.fe_content) AS avg_fe,
       AVG(q.sio2_content) AS avg_sio2,
       COUNT(*) AS samples
FROM fact_ore_quality q
JOIN dim_ore_grade g ON q.ore_grade_id = g.ore_grade_id
WHERE q.date_id BETWEEN 20240101 AND 20240331
GROUP BY g.grade_name;

-- 10.3
EXPLAIN ANALYZE
SELECT e.equipment_name,
       SUM(dt.duration_min) AS total_downtime_min,
       COUNT(*) AS incidents
FROM fact_equipment_downtime dt
JOIN dim_equipment e ON dt.equipment_id = e.equipment_id
WHERE dt.is_planned = FALSE
  AND dt.date_id BETWEEN 20240301 AND 20240331
GROUP BY e.equipment_name
ORDER BY total_downtime_min DESC
LIMIT 5;

-- 10.4
EXPLAIN ANALYZE
SELECT t.date_id, t.time_id, t.sensor_id,
       t.sensor_value, t.quality_flag
FROM fact_equipment_telemetry t
WHERE t.equipment_id = 5
  AND t.is_alarm = TRUE
ORDER BY t.date_id DESC, t.time_id DESC
LIMIT 20;

-- 10.5
EXPLAIN ANALYZE
SELECT p.date_id, e.equipment_name,
       p.tons_mined, p.trips_count, p.operating_hours
FROM fact_production p
JOIN dim_equipment e ON p.equipment_id = e.equipment_id
WHERE p.operator_id = 3
  AND p.date_id BETWEEN 20240311 AND 20240317
ORDER BY p.date_id;

-- Повторное выполнение запросов после создания индексов
-- 10.1
EXPLAIN ANALYZE
SELECT m.mine_name,
       SUM(p.tons_mined) AS total_tons,
       SUM(p.operating_hours) AS total_hours
FROM fact_production p
JOIN dim_mine m ON p.mine_id = m.mine_id
WHERE p.date_id BETWEEN 20240301 AND 20240331
GROUP BY m.mine_name;

-- 10.2
EXPLAIN ANALYZE
SELECT g.grade_name,
       AVG(q.fe_content) AS avg_fe,
       AVG(q.sio2_content) AS avg_sio2,
       COUNT(*) AS samples
FROM fact_ore_quality q
JOIN dim_ore_grade g ON q.ore_grade_id = g.ore_grade_id
WHERE q.date_id BETWEEN 20240101 AND 20240331
GROUP BY g.grade_name;

-- 10.3
EXPLAIN ANALYZE
SELECT e.equipment_name,
       SUM(dt.duration_min) AS total_downtime_min,
       COUNT(*) AS incidents
FROM fact_equipment_downtime dt
JOIN dim_equipment e ON dt.equipment_id = e.equipment_id
WHERE dt.is_planned = FALSE
  AND dt.date_id BETWEEN 20240301 AND 20240331
GROUP BY e.equipment_name
ORDER BY total_downtime_min DESC
LIMIT 5;

-- 10.4
EXPLAIN ANALYZE
SELECT t.date_id, t.time_id, t.sensor_id,
       t.sensor_value, t.quality_flag
FROM fact_equipment_telemetry t
WHERE t.equipment_id = 5
  AND t.is_alarm = TRUE
ORDER BY t.date_id DESC, t.time_id DESC
LIMIT 20;

-- 10.5
EXPLAIN ANALYZE
SELECT p.date_id, e.equipment_name,
       p.tons_mined, p.trips_count, p.operating_hours
FROM fact_production p
JOIN dim_equipment e ON p.equipment_id = e.equipment_id
WHERE p.operator_id = 3
  AND p.date_id BETWEEN 20240311 AND 20240317
ORDER BY p.date_id;


-- Очистка


-- Удаление созданных индексов
DROP INDEX IF EXISTS idx_fact_production_fuel_consumed;
DROP INDEX IF EXISTS idx_fact_telemetry_alarm_date;
DROP INDEX IF EXISTS idx_fact_production_equipment_date;
DROP INDEX IF EXISTS idx_fact_production_date_equipment;
DROP INDEX IF EXISTS idx_dim_operator_lower_last_name;
DROP INDEX IF EXISTS idx_fact_production_date_equipment_include;
DROP INDEX IF EXISTS idx_telemetry_date_brin;
DROP INDEX IF EXISTS idx_fact_downtime_equipment_date_unplanned;
DROP INDEX IF EXISTS idx_fact_production_date_mine;
DROP INDEX IF EXISTS idx_fact_ore_quality_date_grade;
DROP INDEX IF EXISTS idx_fact_downtime_unplanned_date_equipment;
DROP INDEX IF EXISTS idx_fact_telemetry_alarm_equipment_date;
DROP INDEX IF EXISTS idx_fact_production_operator_date_include;

-- Удаление тестовых данных
DELETE FROM fact_production
WHERE date_id IN (20240401, 20240402);
