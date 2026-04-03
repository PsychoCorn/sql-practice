-- Модуль 15: Выполнение хранимых процедур
-- Все объекты создаются в схеме kond

-- Установка search_path для работы в схеме kond
SET search_path TO kond, public;

-- Задание 1. Скалярная функция — расчёт OEE (простое)
CREATE OR REPLACE FUNCTION kond.calc_oee(
    p_operating_hours NUMERIC,
    p_planned_hours NUMERIC,
    p_actual_tons NUMERIC,
    p_target_tons NUMERIC
)
RETURNS NUMERIC
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    v_oee NUMERIC;
BEGIN
    -- Проверка на нулевые значения в знаменателях
    IF p_planned_hours = 0 OR p_target_tons = 0 THEN
        RETURN NULL;
    END IF;

    -- Расчёт OEE по формуле: (рабочие_часы / плановые_часы) * (фактическая_добыча / нормативная_добыча) * 100
    v_oee := (p_operating_hours / p_planned_hours) * (p_actual_tons / p_target_tons) * 100;

    -- Округление до 1 десятичного знака
    RETURN ROUND(v_oee, 1);
END;
$$;

-- Тестирование функции calc_oee
SELECT
    kond.calc_oee(10, 12, 80, 100) AS test1,  -- ожидаемый результат ~66.7
    kond.calc_oee(12, 12, 100, 100) AS test2, -- ожидаемый результат 100.0
    kond.calc_oee(8, 12, 0, 100) AS test3,    -- ожидаемый результат 0.0
    kond.calc_oee(8, 0, 100, 100) AS test4;   -- ожидаемый результат NULL

-- Применение функции к данным fact_production
SELECT
    e.equipment_name,
    fp.operating_hours,
    fp.tons_mined,
    kond.calc_oee(
        fp.operating_hours,
        24.0, -- плановые часы (пример)
        fp.tons_mined,
        150.0 -- нормативная добыча (пример)
    ) AS oee_percentage
FROM fact_production fp
JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
WHERE fp.date_id = 20240115
LIMIT 10;

-- Задание 2. Функция с условной логикой — классификация простоев (простое)
CREATE OR REPLACE FUNCTION kond.classify_downtime(p_duration_min INT)
RETURNS VARCHAR
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RETURN CASE
        WHEN p_duration_min < 15 THEN 'Микропростой'
        WHEN p_duration_min BETWEEN 15 AND 60 THEN 'Краткий простой'
        WHEN p_duration_min BETWEEN 61 AND 240 THEN 'Средний простой'
        WHEN p_duration_min BETWEEN 241 AND 480 THEN 'Длительный простой'
        WHEN p_duration_min > 480 THEN 'Критический простой'
        ELSE 'Неизвестно'
    END;
END;
$$;

-- Применение функции к данным за январь 2024
WITH downtime_data AS (
    SELECT
        kond.classify_downtime(fd.duration_min::INT) AS category,
        fd.duration_min
    FROM fact_equipment_downtime fd
    JOIN dim_date d ON fd.date_id = d.date_id
    WHERE d.year = 2024 AND d.month = 1
)
SELECT
    category,
    COUNT(*) AS cnt,
    ROUND(AVG(duration_min)::numeric, 1) AS avg_duration,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
FROM downtime_data
GROUP BY category
ORDER BY cnt DESC;

-- Задание 3. Табличная функция — детальный отчёт по оборудованию (среднее)
CREATE OR REPLACE FUNCTION kond.get_equipment_summary(
    p_equipment_id INT,
    p_date_from INT,
    p_date_to INT
)
RETURNS TABLE (
    report_date DATE,
    tons_mined NUMERIC,
    trips INT,
    operating_hours NUMERIC,
    fuel_liters NUMERIC,
    tons_per_hour NUMERIC
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    RETURN QUERY
    SELECT
        d.full_date::DATE AS report_date,
        ROUND(SUM(fp.tons_mined)::numeric, 2) AS tons_mined,
        SUM(fp.trips_count)::INT AS trips,
        ROUND(SUM(fp.operating_hours)::numeric, 2) AS operating_hours,
        ROUND(SUM(fp.fuel_consumed_l)::numeric, 2) AS fuel_liters,
        ROUND(
            CASE
                WHEN SUM(fp.operating_hours) > 0
                THEN SUM(fp.tons_mined) / SUM(fp.operating_hours)
                ELSE 0
            END::numeric, 2
        ) AS tons_per_hour
    FROM fact_production fp
    JOIN dim_date d ON fp.date_id = d.date_id
    WHERE fp.equipment_id = p_equipment_id
        AND fp.date_id BETWEEN p_date_from AND p_date_to
    GROUP BY d.full_date
    ORDER BY d.full_date;
END;
$$;

-- Тестирование функции get_equipment_summary
-- Для конкретного оборудования
SELECT * FROM kond.get_equipment_summary(1, 20240101, 20240131) LIMIT 10;

-- В составе JOIN
SELECT
    e.equipment_name,
    s.*
FROM dim_equipment e
CROSS JOIN LATERAL kond.get_equipment_summary(e.equipment_id, 20240101, 20240131) s
WHERE e.mine_id = 1
ORDER BY e.equipment_name, s.report_date
LIMIT 20;

-- Задание 4. Функция с дефолтными параметрами — гибкий фильтр (среднее)
CREATE OR REPLACE FUNCTION kond.get_production_filtered(
    p_date_from INT,
    p_date_to INT,
    p_mine_id INT DEFAULT NULL,
    p_shift_id INT DEFAULT NULL,
    p_equipment_type_id INT DEFAULT NULL
)
RETURNS TABLE (
    mine_name VARCHAR,
    shift_name VARCHAR,
    equipment_type VARCHAR,
    total_tons NUMERIC,
    total_trips BIGINT,
    equip_count BIGINT
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    RETURN QUERY
    SELECT
        m.mine_name,
        s.shift_name,
        et.type_name AS equipment_type,
        ROUND(SUM(fp.tons_mined)::numeric, 2) AS total_tons,
        SUM(fp.trips_count)::BIGINT AS total_trips,
        COUNT(DISTINCT fp.equipment_id)::BIGINT AS equip_count
    FROM fact_production fp
    JOIN dim_mine m ON fp.mine_id = m.mine_id
    JOIN dim_shift s ON fp.shift_id = s.shift_id
    JOIN dim_equipment e ON fp.equipment_id = e.equipment_id
    JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
    WHERE fp.date_id BETWEEN p_date_from AND p_date_to
        AND (p_mine_id IS NULL OR fp.mine_id = p_mine_id)
        AND (p_shift_id IS NULL OR fp.shift_id = p_shift_id)
        AND (p_equipment_type_id IS NULL OR e.equipment_type_id = p_equipment_type_id)
    GROUP BY m.mine_name, s.shift_name, et.type_name
    ORDER BY m.mine_name, s.shift_name, et.type_name;
END;
$$;

-- Тестирование функции get_production_filtered
-- Все данные
SELECT * FROM kond.get_production_filtered(20240101, 20240131) LIMIT 10;

-- Только шахта 1
SELECT * FROM kond.get_production_filtered(20240101, 20240131, p_mine_id := 1);

-- Шахта 1, дневная смена (предполагая, что shift_id = 1 - дневная смена)
SELECT * FROM kond.get_production_filtered(20240101, 20240131, 1, 1);

-- Задание 5. Процедура с транзакциями — архивация данных (среднее)
-- Создание архивной таблицы в схеме kond
CREATE TABLE IF NOT EXISTS kond.archive_telemetry (LIKE fact_equipment_telemetry INCLUDING ALL);

CREATE OR REPLACE PROCEDURE kond.archive_old_telemetry(
    p_before_date_id INT,
    OUT p_archived INT,
    OUT p_deleted INT
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Шаг 1: Копирование записей в архив
    RAISE NOTICE 'Начало архивации данных до date_id = %', p_before_date_id;

    INSERT INTO kond.archive_telemetry
    SELECT * FROM fact_equipment_telemetry
    WHERE date_id < p_before_date_id;

    GET DIAGNOSTICS p_archived = ROW_COUNT;
    RAISE NOTICE 'Скопировано в архив: % записей', p_archived;

    COMMIT;

    -- Шаг 2: Удаление скопированных записей из исходной таблицы
    DELETE FROM fact_equipment_telemetry
    WHERE date_id < p_before_date_id;

    GET DIAGNOSTICS p_deleted = ROW_COUNT;
    RAISE NOTICE 'Удалено из исходной таблицы: % записей', p_deleted;

    COMMIT;

    RAISE NOTICE 'Архивация завершена успешно';
END;
$$;

-- Тестирование процедуры archive_old_telemetry (закомментировано для безопасности)
-- CALL kond.archive_old_telemetry(20240101, NULL, NULL);

-- Проверка данных в архивной таблице
-- SELECT COUNT(*) FROM kond.archive_telemetry;

-- Задание 6. Динамический SQL — универсальный счётчик (среднее)
CREATE OR REPLACE FUNCTION kond.count_fact_records(
    p_table_name TEXT,
    p_date_from INT,
    p_date_to INT
)
RETURNS BIGINT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_count BIGINT;
    v_sql TEXT;
BEGIN
    -- Проверка, что таблица начинается с 'fact_'
    IF NOT p_table_name LIKE 'fact_%' THEN
        RAISE EXCEPTION 'Таблица должна начинаться с ''fact_'', получено: %', p_table_name;
    END IF;

    -- Формирование динамического SQL запроса
    v_sql := format(
        'SELECT COUNT(*) FROM %I WHERE date_id BETWEEN $1 AND $2',
        p_table_name
    );

    -- Выполнение запроса с параметрами
    EXECUTE v_sql INTO v_count USING p_date_from, p_date_to;

    RETURN v_count;
END;
$$;

-- Тестирование функции count_fact_records
SELECT kond.count_fact_records('fact_production', 20240101, 20240131) AS production_count;
SELECT kond.count_fact_records('fact_equipment_downtime', 20240101, 20240131) AS downtime_count;

-- Этот вызов должен вызвать ошибку:
-- SELECT kond.count_fact_records('dim_mine', 20240101, 20240131);

-- Задание 7. Динамический SQL — построитель отчётов (сложное)
CREATE OR REPLACE FUNCTION kond.build_production_report(
    p_group_by TEXT,
    p_date_from INT,
    p_date_to INT,
    p_order_by TEXT DEFAULT 'total_tons DESC'
)
RETURNS TABLE (
    dimension_name VARCHAR,
    total_tons NUMERIC,
    total_trips BIGINT,
    avg_productivity NUMERIC
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_join TEXT;
    v_field TEXT;
    v_order TEXT;
    v_sql TEXT;
BEGIN
    -- Определение JOIN и поля группировки
    CASE p_group_by
        WHEN 'mine' THEN
            v_join := 'JOIN dim_mine d ON fp.mine_id = d.mine_id';
            v_field := 'd.mine_name';
        WHEN 'shift' THEN
            v_join := 'JOIN dim_shift d ON fp.shift_id = d.shift_id';
            v_field := 'd.shift_name';
        WHEN 'operator' THEN
            v_join := 'JOIN dim_operator d ON fp.operator_id = d.operator_id';
            v_field := 'd.last_name || '' '' || LEFT(d.first_name, 1) || ''.''';
        WHEN 'equipment' THEN
            v_join := 'JOIN dim_equipment d ON fp.equipment_id = d.equipment_id';
            v_field := 'd.equipment_name';
        WHEN 'equipment_type' THEN
            v_join := 'JOIN dim_equipment e ON fp.equipment_id = e.equipment_id ' ||
                      'JOIN dim_equipment_type d ON e.equipment_type_id = d.equipment_type_id';
            v_field := 'd.type_name';
        ELSE
            RAISE EXCEPTION 'Некорректное значение p_group_by: %. Допустимые значения: mine, shift, operator, equipment, equipment_type', p_group_by;
    END CASE;

    -- Проверка и установка порядка сортировки
    CASE p_order_by
        WHEN 'total_tons DESC' THEN v_order := 'total_tons DESC';
        WHEN 'total_tons ASC' THEN v_order := 'total_tons ASC';
        WHEN 'dimension_name ASC' THEN v_order := 'dimension_name ASC';
        ELSE
            RAISE EXCEPTION 'Некорректное значение p_order_by: %. Допустимые значения: total_tons DESC, total_tons ASC, dimension_name ASC', p_order_by;
    END CASE;

    -- Формирование и выполнение динамического SQL
    v_sql := format(
        'SELECT %s::VARCHAR AS dimension_name,
                ROUND(SUM(fp.tons_mined)::numeric, 2) AS total_tons,
                SUM(fp.trips_count)::BIGINT AS total_trips,
                ROUND(
                    CASE
                        WHEN SUM(fp.operating_hours) > 0
                        THEN SUM(fp.tons_mined) / SUM(fp.operating_hours)
                        ELSE 0
                    END::numeric, 2
                ) AS avg_productivity
         FROM fact_production fp
         %s
         WHERE fp.date_id BETWEEN $1 AND $2
         GROUP BY %s
         ORDER BY %s',
        v_field, v_join, v_field, v_order
    );

    RETURN QUERY EXECUTE v_sql USING p_date_from, p_date_to;
END;
$$;

-- Тестирование функции build_production_report
SELECT * FROM kond.build_production_report('mine', 20240101, 20240131);
SELECT * FROM kond.build_production_report('shift', 20240101, 20240131);
SELECT * FROM kond.build_production_report('equipment_type', 20240101, 20240131, 'dimension_name ASC');

-- Задание 8. Комплексная процедура — ежедневная загрузка данных (сложное)
-- Создание staging-таблиц в схеме kond
CREATE TABLE IF NOT EXISTS kond.staging_daily_production (
    date_id INT,
    equipment_id INT,
    shift_id INT,
    operator_id INT,
    tons_mined NUMERIC,
    trips_count INT,
    operating_hours NUMERIC,
    fuel_consumed_l NUMERIC,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS kond.staging_rejected (
    date_id INT,
    equipment_id INT,
    shift_id INT,
    operator_id INT,
    tons_mined NUMERIC,
    trips_count INT,
    operating_hours NUMERIC,
    fuel_consumed_l NUMERIC,
    loaded_at TIMESTAMP,
    reject_reason TEXT,
    rejected_at TIMESTAMP DEFAULT NOW()
);

CREATE OR REPLACE PROCEDURE kond.process_daily_production(
    p_date_id INT,
    OUT p_validated INT,
    OUT p_rejected INT,
    OUT p_loaded INT
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Шаг 1: Проверка наличия данных
    IF NOT EXISTS (SELECT 1 FROM kond.staging_daily_production WHERE date_id = p_date_id) THEN
        RAISE EXCEPTION 'Нет данных в staging за date_id = %', p_date_id;
    END IF;

    RAISE NOTICE 'Начало обработки данных за date_id = %', p_date_id;

    -- Шаг 2: Валидация и перемещение невалидных записей
    INSERT INTO kond.staging_rejected (
        date_id, equipment_id, shift_id, operator_id,
        tons_mined, trips_count, operating_hours, fuel_consumed_l,
        loaded_at, reject_reason
    )
    SELECT
        s.date_id, s.equipment_id, s.shift_id, s.operator_id,
        s.tons_mined, s.trips_count, s.operating_hours, s.fuel_consumed_l,
        s.loaded_at,
        CASE
            WHEN s.tons_mined < 0 THEN 'Отрицательная добыча'
            WHEN s.equipment_id NOT IN (SELECT equipment_id FROM dim_equipment) THEN 'Несуществующее оборудование'
            WHEN s.operator_id NOT IN (SELECT operator_id FROM dim_operator) THEN 'Несуществующий оператор'
            WHEN s.trips_count < 0 THEN 'Отрицательное количество рейсов'
            WHEN s.operating_hours < 0 THEN 'Отрицательное рабочее время'
            WHEN s.fuel_consumed_l < 0 THEN 'Отрицательный расход топлива'
            ELSE 'Неизвестная причина'
        END AS reject_reason
    FROM kond.staging_daily_production s
    WHERE s.date_id = p_date_id
      AND (s.tons_mined < 0
           OR s.equipment_id NOT IN (SELECT equipment_id FROM dim_equipment)
           OR s.operator_id NOT IN (SELECT operator_id FROM dim_operator)
           OR s.trips_count < 0
           OR s.operating_hours < 0
           OR s.fuel_consumed_l < 0);

    GET DIAGNOSTICS p_rejected = ROW_COUNT;
    RAISE NOTICE 'Отбраковано: % записей', p_rejected;

    -- Подсчёт валидных записей
    SELECT COUNT(*) INTO p_validated
    FROM kond.staging_daily_production s
    WHERE s.date_id = p_date_id
      AND s.tons_mined >= 0
      AND s.equipment_id IN (SELECT equipment_id FROM dim_equipment)
      AND s.operator_id IN (SELECT operator_id FROM dim_operator)
      AND s.trips_count >= 0
      AND s.operating_hours >= 0
      AND s.fuel_consumed_l >= 0;

    RAISE NOTICE 'Валидных записей: %', p_validated;

    COMMIT;

    -- Шаг 3: Удаление старых данных из fact_production за эту дату
    DELETE FROM fact_production WHERE date_id = p_date_id;
    RAISE NOTICE 'Удалены старые данные за date_id = %', p_date_id;

    -- Шаг 4: Вставка валидных записей в fact_production
    INSERT INTO fact_production (
        date_id, equipment_id, shift_id, operator_id,
        tons_mined, trips_count, operating_hours, fuel_consumed_l,
        mine_id, shaft_id, location_id, ore_grade_id,
        tons_transported, distance_km, loaded_at
    )
    SELECT
        s.date_id,
        s.equipment_id,
        s.shift_id,
        s.operator_id,
        s.tons_mined,
        s.trips_count,
        s.operating_hours,
        s.fuel_consumed_l,
        e.mine_id,
        1 AS shaft_id, -- примерное значение, можно адаптировать
        1 AS location_id, -- примерное значение
        1 AS ore_grade_id, -- примерное значение
        s.tons_mined AS tons_transported, -- примерное значение
        0 AS distance_km, -- примерное значение
        NOW() AS loaded_at
    FROM kond.staging_daily_production s
    JOIN dim_equipment e ON s.equipment_id = e.equipment_id
    WHERE s.date_id = p_date_id
      AND s.tons_mined >= 0
      AND s.equipment_id IN (SELECT equipment_id FROM dim_equipment)
      AND s.operator_id IN (SELECT operator_id FROM dim_operator)
      AND s.trips_count >= 0
      AND s.operating_hours >= 0
      AND s.fuel_consumed_l >= 0;

    GET DIAGNOSTICS p_loaded = ROW_COUNT;
    RAISE NOTICE 'Загружено в fact_production: % записей', p_loaded;

    COMMIT;

    RAISE NOTICE 'Обработка данных за date_id = % завершена успешно', p_date_id;
END;
$$;

-- Тестирование процедуры process_daily_production (закомментировано для безопасности)
-- Вставка тестовых данных в staging
/*
INSERT INTO kond.staging_daily_production (date_id, equipment_id, shift_id, operator_id, tons_mined, trips_count, operating_hours, fuel_consumed_l)
VALUES
    (20240115, 1, 1, 1, 150.5, 15, 10.5, 120.0),  -- корректная запись
    (20240115, 1, 1, 1, -10.0, 5, 8.0, 80.0),     -- отрицательная добыча
    (20240115, 999, 1, 1, 100.0, 10, 9.0, 90.0),  -- несуществующее оборудование
    (20240115, 1, 1, 999, 120.0, 12, 9.5, 95.0);  -- несуществующий оператор

-- Вызов процедуры
CALL kond.process_daily_production(20240115, NULL, NULL, NULL);

-- Проверка результатов
SELECT * FROM fact_production WHERE date_id = 20240115;
SELECT * FROM kond.staging_rejected WHERE date_id = 20240115;
*/

-- Восстановление search_path к значению по умолчанию
RESET search_path;
