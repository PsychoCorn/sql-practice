-- Модуль 17: Применение обработки ошибок

SET search_path TO kond, public;

CREATE TABLE IF NOT EXISTS kond.error_log (
    log_id      SERIAL PRIMARY KEY,
    log_time    TIMESTAMP DEFAULT NOW(),
    severity    VARCHAR(20),
    source      VARCHAR(100),
    sqlstate    VARCHAR(5),
    message     TEXT,
    detail      TEXT,
    hint        TEXT,
    context     TEXT,
    username    VARCHAR(100) DEFAULT CURRENT_USER,
    parameters  JSONB
);

CREATE OR REPLACE FUNCTION kond.log_error(
    p_severity VARCHAR, p_source VARCHAR,
    p_sqlstate VARCHAR DEFAULT NULL, p_message TEXT DEFAULT NULL,
    p_detail TEXT DEFAULT NULL, p_hint TEXT DEFAULT NULL,
    p_context TEXT DEFAULT NULL, p_parameters JSONB DEFAULT NULL
)
RETURNS INT LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE v_log_id INT;
BEGIN
    INSERT INTO kond.error_log (severity, source, sqlstate, message, detail, hint, context, parameters)
    VALUES (p_severity, p_source, p_sqlstate, p_message, p_detail, p_hint, p_context, p_parameters)
    RETURNING log_id INTO v_log_id;
    RETURN v_log_id;
END;
$$;

-- Задание 1. Безопасное деление
CREATE OR REPLACE FUNCTION kond.safe_production_rate(
    p_tons NUMERIC,
    p_hours NUMERIC
)
RETURNS NUMERIC
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF p_tons IS NULL OR p_hours IS NULL THEN
        RETURN NULL;
    END IF;

    BEGIN
        RETURN ROUND(p_tons / p_hours, 2);
    EXCEPTION
        WHEN division_by_zero THEN
            RAISE WARNING 'деление на ноль при расчёте производительности: tons=%, hours=%', p_tons, p_hours;
            RETURN 0;
    END;
END;
$$;

SELECT
    kond.safe_production_rate(150, 8) AS test1,
    kond.safe_production_rate(150, 0) AS test2,
    kond.safe_production_rate(NULL, 8) AS test3;


SELECT
    equipment_id,
    tons_mined,
    operating_hours,
    kond.safe_production_rate(tons_mined, operating_hours) AS rate
FROM fact_production
WHERE date_id = 20250115
ORDER BY rate DESC
LIMIT 10;

-- Задание 2. Валидация данных телеметрии (простое)
CREATE OR REPLACE FUNCTION kond.validate_sensor_reading(
    p_sensor_type VARCHAR,
    p_value NUMERIC
)
RETURNS VARCHAR
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    v_min_value NUMERIC;
    v_max_value NUMERIC;
BEGIN

    CASE p_sensor_type
        WHEN 'Температура' THEN
            v_min_value := -40;
            v_max_value := 200;
        WHEN 'Давление' THEN
            v_min_value := 0;
            v_max_value := 500;
        WHEN 'Вибрация' THEN
            v_min_value := 0;
            v_max_value := 100;
        WHEN 'Скорость' THEN
            v_min_value := 0;
            v_max_value := 50;
        ELSE
            RAISE EXCEPTION 'S0001' USING
                MESSAGE = 'Неизвестный тип датчика: ' || p_sensor_type,
                HINT = 'Допустимые типы: Температура, Давление, Вибрация, Скорость';
    END CASE;


    IF p_value < v_min_value OR p_value > v_max_value THEN
        RAISE EXCEPTION 'S0002' USING
            MESSAGE = 'Значение вне допустимого диапазона',
            HINT = format('Допустимый диапазон для %s: %s..%s', p_sensor_type, v_min_value, v_max_value);
    END IF;

    RETURN 'OK';
END;
$$;


DO $$
BEGIN

    RAISE NOTICE 'validate_sensor_reading(''Температура'', 85) = %', kond.validate_sensor_reading('Температура', 85);
    RAISE NOTICE 'validate_sensor_reading(''Давление'', 300) = %', kond.validate_sensor_reading('Давление', 300);


    /*
    RAISE NOTICE 'validate_sensor_reading(''Температура'', 250) = %', kond.validate_sensor_reading('Температура', 250);
    RAISE NOTICE 'validate_sensor_reading(''Вибрация'', 150) = %', kond.validate_sensor_reading('Вибрация', 150);
    RAISE NOTICE 'validate_sensor_reading(''Неизвестный'', 50) = %', kond.validate_sensor_reading('Неизвестный', 50);
    */
END;
$$;

-- Задание 3. Обработка ошибок при вставке (среднее)
DO $$
DECLARE
    v_record_num INT := 1;
    v_success_count INT := 0;
    v_error_count INT := 0;
    v_log_id INT;
BEGIN
    RAISE NOTICE 'Начало пакетной вставки записей в fact_equipment_downtime';


    BEGIN
        INSERT INTO fact_equipment_downtime (
            downtime_id, date_id, shift_id, equipment_id, reason_id,
            start_time, end_time, duration_min, is_planned, comment
        ) VALUES (
            1000001, 20250115, 1, 1, 1,
            '2025-01-15 08:00:00', '2025-01-15 10:30:00', 150, true, 'Плановое ТО'
        );
        v_success_count := v_success_count + 1;
        RAISE NOTICE 'Запись %: успешно вставлена', v_record_num;
    EXCEPTION
        WHEN OTHERS THEN
            v_error_count := v_error_count + 1;
            v_log_id := kond.log_error('ERROR', 'batch_insert_downtime', SQLSTATE, SQLERRM,
                                     SQLERRM, NULL, NULL,
                                     jsonb_build_object('record_num', v_record_num, 'error_type', 'general'));
            RAISE WARNING 'Запись %: ошибка - %', v_record_num, SQLERRM;
    END;
    v_record_num := v_record_num + 1;


    BEGIN
        INSERT INTO fact_equipment_downtime (
            downtime_id, date_id, shift_id, equipment_id, reason_id,
            start_time, end_time, duration_min, is_planned, comment
        ) VALUES (
            1000002, 20250115, 1, 2, 2,
            '2025-01-15 12:00:00', '2025-01-15 13:30:00', 90, false, 'Заправка'
        );
        v_success_count := v_success_count + 1;
        RAISE NOTICE 'Запись %: успешно вставлена', v_record_num;
    EXCEPTION
        WHEN OTHERS THEN
            v_error_count := v_error_count + 1;
            v_log_id := kond.log_error('ERROR', 'batch_insert_downtime', SQLSTATE, SQLERRM,
                                     SQLERRM, NULL, NULL,
                                     jsonb_build_object('record_num', v_record_num, 'error_type', 'general'));
            RAISE WARNING 'Запись %: ошибка - %', v_record_num, SQLERRM;
    END;
    v_record_num := v_record_num + 1;


    BEGIN
        INSERT INTO fact_equipment_downtime (
            downtime_id, date_id, shift_id, equipment_id, reason_id,
            start_time, end_time, duration_min, is_planned, comment
        ) VALUES (
            1000003, 20250115, 1, 9999, 1,
            '2025-01-15 14:00:00', '2025-01-15 15:00:00', 60, true, 'Тест FK violation'
        );
        v_success_count := v_success_count + 1;
        RAISE NOTICE 'Запись %: успешно вставлена', v_record_num;
    EXCEPTION
        WHEN foreign_key_violation THEN
            v_error_count := v_error_count + 1;
            v_log_id := kond.log_error('ERROR', 'batch_insert_downtime', SQLSTATE, SQLERRM,
                                     SQLERRM, NULL, NULL,
                                     jsonb_build_object('record_num', v_record_num, 'error_type', 'fk_violation'));
            RAISE WARNING 'Запись %: ошибка FK violation - %', v_record_num, SQLERRM;
        WHEN OTHERS THEN
            v_error_count := v_error_count + 1;
            v_log_id := kond.log_error('ERROR', 'batch_insert_downtime', SQLSTATE, SQLERRM,
                                     SQLERRM, NULL, NULL,
                                     jsonb_build_object('record_num', v_record_num, 'error_type', 'general'));
            RAISE WARNING 'Запись %: ошибка - %', v_record_num, SQLERRM;
    END;
    v_record_num := v_record_num + 1;


    BEGIN
        INSERT INTO fact_equipment_downtime (
            downtime_id, date_id, shift_id, equipment_id, reason_id,
            start_time, end_time, duration_min, is_planned, comment
        ) VALUES (
            1000004, NULL, 1, 1, 1,
            '2025-01-15 16:00:00', '2025-01-15 17:00:00', 60, true, 'Тест NOT NULL violation'
        );
        v_success_count := v_success_count + 1;
        RAISE NOTICE 'Запись %: успешно вставлена', v_record_num;
    EXCEPTION
        WHEN not_null_violation THEN
            v_error_count := v_error_count + 1;
            v_log_id := kond.log_error('ERROR', 'batch_insert_downtime', SQLSTATE, SQLERRM,
                                     SQLERRM, NULL, NULL,
                                     jsonb_build_object('record_num', v_record_num, 'error_type', 'not_null_violation'));
            RAISE WARNING 'Запись %: ошибка NOT NULL violation - %', v_record_num, SQLERRM;
        WHEN OTHERS THEN
            v_error_count := v_error_count + 1;
            v_log_id := kond.log_error('ERROR', 'batch_insert_downtime', SQLSTATE, SQLERRM,
                                     SQLERRM, NULL, NULL,
                                     jsonb_build_object('record_num', v_record_num, 'error_type', 'general'));
            RAISE WARNING 'Запись %: ошибка - %', v_record_num, SQLERRM;
    END;
    v_record_num := v_record_num + 1;


    BEGIN
        INSERT INTO fact_equipment_downtime (
            downtime_id, date_id, shift_id, equipment_id, reason_id,
            start_time, end_time, duration_min, is_planned, comment
        ) VALUES (
            1000001, 20250115, 1, 1, 1,
            '2025-01-15 18:00:00', '2025-01-15 19:00:00', 60, true, 'Тест UNIQUE violation'
        );
        v_success_count := v_success_count + 1;
        RAISE NOTICE 'Запись %: успешно вставлена', v_record_num;
    EXCEPTION
        WHEN unique_violation THEN
            v_error_count := v_error_count + 1;
            v_log_id := kond.log_error('ERROR', 'batch_insert_downtime', SQLSTATE, SQLERRM,
                                     SQLERRM, NULL, NULL,
                                     jsonb_build_object('record_num', v_record_num, 'error_type', 'unique_violation'));
            RAISE WARNING 'Запись %: ошибка UNIQUE violation - %', v_record_num, SQLERRM;
        WHEN OTHERS THEN
            v_error_count := v_error_count + 1;
            v_log_id := kond.log_error('ERROR', 'batch_insert_downtime', SQLSTATE, SQLERRM,
                                     SQLERRM, NULL, NULL,
                                     jsonb_build_object('record_num', v_record_num, 'error_type', 'general'));
            RAISE WARNING 'Запись %: ошибка - %', v_record_num, SQLERRM;
    END;


    RAISE NOTICE '';
    RAISE NOTICE 'Статистика пакетной вставки:';
    RAISE NOTICE 'Успешно вставлено: % записей', v_success_count;
    RAISE NOTICE 'Ошибок: % записей', v_error_count;
    RAISE NOTICE 'Всего обработано: % записей', v_record_num - 1;
END;
$$;

-- Задание 4. GET STACKED DIAGNOSTICS — детальный отчёт (среднее)
CREATE OR REPLACE FUNCTION kond.test_error_diagnostics(
    p_error_type INT
)
RETURNS TABLE (
    field_name VARCHAR,
    field_value TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN

    CASE p_error_type
        WHEN 1 THEN

            PERFORM 1 / 0;
        WHEN 2 THEN

            BEGIN
                INSERT INTO dim_mine (mine_id, mine_name, mine_code)
                VALUES (1, 'Тестовая шахта', 'TEST');
            EXCEPTION WHEN unique_violation THEN
                RAISE;
            END;
        WHEN 3 THEN

            INSERT INTO fact_production (production_id, date_id, shift_id, equipment_id, mine_id, shaft_id, tons_mined)
            VALUES (999999, 20250115, 1, 9999, 1, 1, 100);
        WHEN 4 THEN

            PERFORM 'не число'::INT;
        WHEN 5 THEN

            RAISE EXCEPTION 'Пользовательская ошибка: тестовая ошибка с кодом %', p_error_type;
        ELSE
            RAISE EXCEPTION 'Неизвестный тип ошибки: %', p_error_type;
    END CASE;


    RETURN QUERY SELECT 'NO_ERROR'::VARCHAR, 'Функция выполнена без ошибок'::TEXT;
    RETURN;

EXCEPTION
    WHEN OTHERS THEN

        RETURN QUERY
        SELECT 'RETURNED_SQLSTATE'::VARCHAR, COALESCE(RETURNED_SQLSTATE::TEXT, '') UNION ALL
        SELECT 'MESSAGE_TEXT', COALESCE(MESSAGE_TEXT, '') UNION ALL
        SELECT 'PG_EXCEPTION_DETAIL', COALESCE(PG_EXCEPTION_DETAIL, '') UNION ALL
        SELECT 'PG_EXCEPTION_HINT', COALESCE(PG_EXCEPTION_HINT, '') UNION ALL
        SELECT 'PG_EXCEPTION_CONTEXT', COALESCE(PG_EXCEPTION_CONTEXT, '') UNION ALL
        SELECT 'COLUMN_NAME', COALESCE(COLUMN_NAME, '') UNION ALL
        SELECT 'CONSTRAINT_NAME', COALESCE(CONSTRAINT_NAME, '') UNION ALL
        SELECT 'DATATYPE_NAME', COALESCE(DATATYPE_NAME, '') UNION ALL
        SELECT 'TABLE_NAME', COALESCE(TABLE_NAME, '') UNION ALL
        SELECT 'SCHEMA_NAME', COALESCE(SCHEMA_NAME, '');
END;
$$;

-- Задание 5. Безопасный импорт с логированием (среднее)

CREATE TABLE IF NOT EXISTS kond.staging_lab_results (
    row_id       SERIAL PRIMARY KEY,
    mine_name    TEXT,
    sample_date  TEXT,
    fe_content   TEXT,
    moisture     TEXT,
    status       VARCHAR(20) DEFAULT 'NEW',
    error_msg    TEXT
);


INSERT INTO kond.staging_lab_results (mine_name, sample_date, fe_content, moisture) VALUES
    ('Шахта "Северная"', '15-01-2025', '53.5', '2.1'),
    ('Несуществующая', '15-01-2025', '48.2', '1.8'),
    ('Шахта "Южная"', '32-01-2025', '45.7', '2.3'),
    ('Шахта "Северная"', '16-01-2025', '55.1', '1.9'),
    ('Шахта "Южная"', '17-01-2025', 'N/A', '2.0'),
    ('Шахта "Северная"', '18-01-2025', '150', '1.7'),
    ('Шахта "Южная"', '19-01-2025', '47.8', '2.2'),
    ('Шахта "Северная"', '20-01-2025', '52.3', '1.8'),
    ('Шахта "Южная"', '21-01-2025', '46.5', '2.1'),
    ('Шахта "Северная"', '22-01-2025', '54.2', '1.9');

CREATE OR REPLACE FUNCTION kond.process_lab_import()
RETURNS TABLE (
    total_processed INT,
    valid_count INT,
    error_count INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_record RECORD;
    v_mine_id INT;
    v_date_id INT;
    v_fe_content NUMERIC;
    v_moisture NUMERIC;
    v_error_msg TEXT;
    v_total INT := 0;
    v_valid INT := 0;
    v_errors INT := 0;
    v_log_id INT;
BEGIN
    RAISE NOTICE 'Начало обработки импорта лабораторных данных...';


    FOR v_record IN
        SELECT row_id, mine_name, sample_date, fe_content, moisture
        FROM kond.staging_lab_results
        WHERE status = 'NEW'
        ORDER BY row_id
    LOOP
        v_total := v_total + 1;
        v_error_msg := NULL;

        BEGIN

            SELECT mine_id INTO v_mine_id
            FROM dim_mine
            WHERE mine_name = v_record.mine_name;

            IF v_mine_id IS NULL THEN
                RAISE EXCEPTION 'Шахта "%" не найдена', v_record.mine_name;
            END IF;


            BEGIN
                v_date_id := EXTRACT(YEAR FROM v_record.sample_date::date) * 10000 +
                            EXTRACT(MONTH FROM v_record.sample_date::date) * 100 +
                            EXTRACT(DAY FROM v_record.sample_date::date);
            EXCEPTION
                WHEN invalid_datetime_format OR invalid_text_representation THEN
                    RAISE EXCEPTION 'Некорректная дата: "%"', v_record.sample_date;
            END;


            BEGIN
                v_fe_content := v_record.fe_content::NUMERIC;
            EXCEPTION
                WHEN invalid_text_representation THEN
                    RAISE EXCEPTION 'fe_content = "%" — не является числом', v_record.fe_content;
            END;


            IF v_fe_content < 0 OR v_fe_content > 100 THEN
                RAISE EXCEPTION 'fe_content = % — вне допустимого диапазона 0..100', v_fe_content;
            END IF;


            BEGIN
                v_moisture := v_record.moisture::NUMERIC;
            EXCEPTION
                WHEN invalid_text_representation THEN
                    RAISE EXCEPTION 'moisture = "%" — не является числом', v_record.moisture;
            END;


            IF v_moisture < 0 OR v_moisture > 100 THEN
                RAISE EXCEPTION 'moisture = % — вне допустимого диапазона 0..100', v_moisture;
            END IF;


            UPDATE kond.staging_lab_results
            SET status = 'VALID',
                error_msg = NULL
            WHERE row_id = v_record.row_id;

            v_valid := v_valid + 1;
            RAISE NOTICE 'Запись %: ВАЛИДНА', v_record.row_id;

        EXCEPTION
            WHEN OTHERS THEN
                v_error_msg := SQLERRM;
                v_errors := v_errors + 1;


                UPDATE kond.staging_lab_results
                SET status = 'ERROR',
                    error_msg = v_error_msg
                WHERE row_id = v_record.row_id;


                v_log_id := kond.log_error(
                    'ERROR', 'process_lab_import', SQLSTATE, SQLERRM,
                    NULL, NULL, NULL,
                    jsonb_build_object(
                        'row_id', v_record.row_id,
                        'mine_name', v_record.mine_name,
                        'sample_date', v_record.sample_date,
                        'fe_content', v_record.fe_content,
                        'moisture', v_record.moisture
                    )
                );

                RAISE NOTICE 'Запись %: ОШИБКА - %', v_record.row_id, v_error_msg;
        END;
    END LOOP;


    total_processed := v_total;
    valid_count := v_valid;
    error_count := v_errors;

    RAISE NOTICE '';
    RAISE NOTICE 'Обработка завершена:';
    RAISE NOTICE 'Всего обработано: %', v_total;
    RAISE NOTICE 'Валидных: %', v_valid;
    RAISE NOTICE 'Ошибок: %', v_errors;

    RETURN NEXT;
    RETURN;
END;
$$;


SELECT * FROM kond.process_lab_import();


SELECT * FROM kond.staging_lab_results ORDER BY row_id;


SELECT * FROM kond.error_log WHERE source = 'process_lab_import' ORDER BY log_id DESC;

-- Задание 6. Комплексная функция с иерархией обработки ошибок (сложное)
CREATE TABLE IF NOT EXISTS kond.daily_kpi (
    kpi_id         SERIAL PRIMARY KEY,
    mine_id        INT,
    date_id        INT,
    tons_mined     NUMERIC,
    oee_percent    NUMERIC,
    downtime_hours NUMERIC,
    quality_score  NUMERIC,
    status         VARCHAR(20),
    error_detail   TEXT,
    calculated_at  TIMESTAMP DEFAULT NOW(),
    UNIQUE (mine_id, date_id)
);

CREATE OR REPLACE FUNCTION kond.recalculate_daily_kpi(p_date_id INT)
RETURNS TABLE (
    mines_processed INT,
    mines_ok INT,
    mines_error INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_mine_record RECORD;
    v_tons_mined NUMERIC;
    v_oee_percent NUMERIC;
    v_downtime_hours NUMERIC;
    v_quality_score NUMERIC;
    v_equipment_count INT;
    v_planned_hours NUMERIC;
    v_processed INT := 0;
    v_ok INT := 0;
    v_error INT := 0;
    v_error_detail TEXT;
    v_log_id INT;
BEGIN
    RAISE NOTICE 'Начало пересчёта KPI за date_id = %', p_date_id;


    FOR v_mine_record IN
        SELECT mine_id, mine_name
        FROM dim_mine
        ORDER BY mine_id
    LOOP
        v_processed := v_processed + 1;
        v_error_detail := NULL;

        BEGIN

            BEGIN

                SELECT COALESCE(SUM(tons_mined), 0) INTO v_tons_mined
                FROM fact_production
                WHERE date_id = p_date_id AND mine_id = v_mine_record.mine_id;


                SELECT COUNT(DISTINCT equipment_id) INTO v_equipment_count
                FROM fact_production
                WHERE date_id = p_date_id AND mine_id = v_mine_record.mine_id;

                v_planned_hours := v_equipment_count * 12;


                SELECT
                    CASE WHEN SUM(operating_hours) > 0 AND v_planned_hours > 0
                         THEN ROUND((SUM(operating_hours) / v_planned_hours) * 100, 1)
                         ELSE 0
                    END
                INTO v_oee_percent
                FROM fact_production
                WHERE date_id = p_date_id AND mine_id = v_mine_record.mine_id;


                SELECT COALESCE(SUM(duration_min) / 60.0, 0) INTO v_downtime_hours
                FROM fact_equipment_downtime
                WHERE date_id = p_date_id
                  AND equipment_id IN (
                      SELECT equipment_id FROM dim_equipment WHERE mine_id = v_mine_record.mine_id
                  );


                SELECT COALESCE(ROUND(AVG(fe_content)::numeric, 2), 0) INTO v_quality_score
                FROM fact_ore_quality
                WHERE date_id = p_date_id AND mine_id = v_mine_record.mine_id;


                INSERT INTO kond.daily_kpi (
                    mine_id, date_id, tons_mined, oee_percent,
                    downtime_hours, quality_score, status
                ) VALUES (
                    v_mine_record.mine_id, p_date_id, v_tons_mined, v_oee_percent,
                    v_downtime_hours, v_quality_score, 'OK'
                )
                ON CONFLICT (mine_id, date_id) DO UPDATE SET
                    tons_mined = EXCLUDED.tons_mined,
                    oee_percent = EXCLUDED.oee_percent,
                    downtime_hours = EXCLUDED.downtime_hours,
                    quality_score = EXCLUDED.quality_score,
                    status = EXCLUDED.status,
                    error_detail = NULL,
                    calculated_at = NOW();

                v_ok := v_ok + 1;
                RAISE NOTICE 'Шахта % (%): KPI успешно рассчитан', v_mine_record.mine_name, v_mine_record.mine_id;

            EXCEPTION
                WHEN OTHERS THEN
                    v_error_detail := SQLERRM;
                    RAISE;
            END;

        EXCEPTION
            WHEN OTHERS THEN
                v_error := v_error + 1;
                v_error_detail := SQLERRM;


                INSERT INTO kond.daily_kpi (
                    mine_id, date_id, status, error_detail
                ) VALUES (
                    v_mine_record.mine_id, p_date_id, 'ERROR', v_error_detail
                )
                ON CONFLICT (mine_id, date_id) DO UPDATE SET
                    status = 'ERROR',
                    error_detail = v_error_detail,
                    calculated_at = NOW();


                v_log_id := kond.log_error(
                    'ERROR', 'recalculate_daily_kpi', SQLSTATE, SQLERRM,
                    NULL, NULL, NULL,
                    jsonb_build_object(
                        'mine_id', v_mine_record.mine_id,
                        'mine_name', v_mine_record.mine_name,
                        'date_id', p_date_id
                    )
                );

                RAISE WARNING 'Шахта % (%): ошибка расчёта KPI - %',
                    v_mine_record.mine_name, v_mine_record.mine_id, v_error_detail;
        END;
    END LOOP;


    mines_processed := v_processed;
    mines_ok := v_ok;
    mines_error := v_error;

    RAISE NOTICE '';
    RAISE NOTICE 'Пересчёт KPI завершён:';
    RAISE NOTICE 'Обработано шахт: %', v_processed;
    RAISE NOTICE 'Успешно: %', v_ok;
    RAISE NOTICE 'С ошибками: %', v_error;

    RETURN NEXT;
    RETURN;
END;
$$;


SELECT * FROM kond.recalculate_daily_kpi(20250115);


SELECT * FROM kond.daily_kpi WHERE date_id = 20250115 ORDER BY mine_id;


RESET search_path;
