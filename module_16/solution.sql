-- Модуль 16: Программирование при помощи SQL
-- Все объекты создаются в схеме kond

-- Установка search_path для работы в схеме kond
SET search_path TO kond, public;

-- Задание 1. Анонимный блок — статистика по шахтам (простое)
DO $$
DECLARE
    v_mine_count INT;
    v_total_tons NUMERIC;
    v_avg_fe NUMERIC;
    v_downtime_count INT;
BEGIN
    -- Количество шахт
    SELECT COUNT(*) INTO v_mine_count FROM dim_mine;

    -- Общая добыча за январь 2025
    SELECT COALESCE(SUM(fp.tons_mined), 0) INTO v_total_tons
    FROM fact_production fp
    JOIN dim_date d ON fp.date_id = d.date_id
    WHERE d.year = 2025 AND d.month = 1;

    -- Среднее содержание Fe за январь 2025
    SELECT COALESCE(ROUND(AVG(fq.fe_content)::numeric, 1), 0) INTO v_avg_fe
    FROM fact_ore_quality fq
    JOIN dim_date d ON fq.date_id = d.date_id
    WHERE d.year = 2025 AND d.month = 1;

    -- Количество простоев за январь 2025
    SELECT COUNT(*) INTO v_downtime_count
    FROM fact_equipment_downtime fd
    JOIN dim_date d ON fd.date_id = d.date_id
    WHERE d.year = 2025 AND d.month = 1;

    -- Вывод форматированного отчёта
    RAISE NOTICE '===== Сводка по предприятию «Руда+» =====';
    RAISE NOTICE 'Количество шахт: %', v_mine_count;
    RAISE NOTICE 'Добыча за январь 2025: % т', ROUND(v_total_tons, 1);
    RAISE NOTICE 'Среднее содержание Fe: % %%', v_avg_fe;
    RAISE NOTICE 'Количество простоев: %', v_downtime_count;
    RAISE NOTICE '==========================================';
END;
$$;

-- Задание 2. Переменные и классификация — категории оборудования (простое)
DO $$
DECLARE
    v_equipment RECORD;
    v_age_years INT;
    v_category VARCHAR(50);
    v_new_count INT := 0;
    v_working_count INT := 0;
    v_attention_count INT := 0;
    v_replace_count INT := 0;
BEGIN
    RAISE NOTICE 'Классификация оборудования по возрасту:';
    RAISE NOTICE '----------------------------------------';

    FOR v_equipment IN
        SELECT
            e.equipment_name,
            et.type_name,
            e.commissioning_date,
            CASE
                WHEN e.commissioning_date IS NOT NULL
                THEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, e.commissioning_date))
                ELSE EXTRACT(YEAR FROM AGE(CURRENT_DATE, CURRENT_DATE - (random() * 4000)::INT))
            END AS age_years
        FROM dim_equipment e
        JOIN dim_equipment_type et ON e.equipment_type_id = et.equipment_type_id
        ORDER BY e.equipment_name
    LOOP
        v_age_years := v_equipment.age_years;

        -- Классификация по возрасту
        IF v_age_years < 2 THEN
            v_category := 'Новое';
            v_new_count := v_new_count + 1;
        ELSIF v_age_years BETWEEN 2 AND 5 THEN
            v_category := 'Рабочее';
            v_working_count := v_working_count + 1;
        ELSIF v_age_years BETWEEN 6 AND 10 THEN
            v_category := 'Требует внимания';
            v_attention_count := v_attention_count + 1;
        ELSE
            v_category := 'На замену';
            v_replace_count := v_replace_count + 1;
        END IF;

        RAISE NOTICE '%-20s | %-30s | %-3s лет | %-20s',
            v_equipment.equipment_name,
            v_equipment.type_name,
            v_age_years,
            v_category;
    END LOOP;

    -- Вывод сводки
    RAISE NOTICE '';
    RAISE NOTICE 'Сводка по категориям:';
    RAISE NOTICE '---------------------';
    RAISE NOTICE 'Новое:              %', v_new_count;
    RAISE NOTICE 'Рабочее:            %', v_working_count;
    RAISE NOTICE 'Требует внимания:   %', v_attention_count;
    RAISE NOTICE 'На замену:          %', v_replace_count;
END;
$$;

-- Задание 3. Циклы — подневной анализ добычи (простое)
DO $$
DECLARE
    v_day INT;
    v_date_id INT;
    v_daily_tons NUMERIC;
    v_running_total NUMERIC := 0;
    v_avg_previous NUMERIC := 0;
    v_total_tons NUMERIC := 0;
    v_best_day INT := 0;
    v_best_tons NUMERIC := 0;
    v_day_count INT := 0;
BEGIN
    RAISE NOTICE 'Подневной анализ добычи за первые 2 недели января 2025:';
    RAISE NOTICE '--------------------------------------------------------';

    FOR v_day IN 1..14 LOOP
        v_date_id := 20250100 + v_day;

        -- Суммарная добыча за день
        SELECT COALESCE(SUM(tons_mined), 0) INTO v_daily_tons
        FROM fact_production
        WHERE date_id = v_date_id;

        -- Нарастающий итог
        v_running_total := v_running_total + v_daily_tons;
        v_total_tons := v_total_tons + v_daily_tons;
        v_day_count := v_day_count + 1;

        -- Проверка на рекорд
        IF v_day = 1 THEN
            v_avg_previous := v_daily_tons;
            RAISE NOTICE 'День %: % т | Нарастающий: % т |',
                LPAD(v_day::text, 2, '0'),
                ROUND(v_daily_tons, 1),
                ROUND(v_running_total, 1);
        ELSE
            IF v_daily_tons > v_avg_previous THEN
                RAISE NOTICE 'День %: % т | Нарастающий: % т | РЕКОРД',
                    LPAD(v_day::text, 2, '0'),
                    ROUND(v_daily_tons, 1),
                    ROUND(v_running_total, 1);
            ELSE
                RAISE NOTICE 'День %: % т | Нарастающий: % т |',
                    LPAD(v_day::text, 2, '0'),
                    ROUND(v_daily_tons, 1),
                    ROUND(v_running_total, 1);
            END IF;
            v_avg_previous := v_running_total / v_day;
        END IF;

        -- Лучший день
        IF v_daily_tons > v_best_tons THEN
            v_best_tons := v_daily_tons;
            v_best_day := v_day;
        END IF;
    END LOOP;

    -- Итоги
    RAISE NOTICE '';
    RAISE NOTICE 'Итоги:';
    RAISE NOTICE '------';
    RAISE NOTICE 'Общий итог: % т', ROUND(v_total_tons, 1);
    RAISE NOTICE 'Средняя добыча в день: % т', ROUND(v_total_tons / 14, 1);
    RAISE NOTICE 'Лучший день: % января (% т)', v_best_day, ROUND(v_best_tons, 1);
END;
$$;

-- Задание 4. WHILE — мониторинг порога простоев (среднее)
DO $$
DECLARE
    v_current_date_id INT := 20250101;
    v_threshold NUMERIC := 500; -- часов
    v_cumulative_hours NUMERIC := 0;
    v_daily_hours NUMERIC;
    v_found BOOLEAN := FALSE;
BEGIN
    RAISE NOTICE 'Мониторинг порога простоев (порог: % часов):', v_threshold;
    RAISE NOTICE '---------------------------------------------';

    WHILE v_current_date_id <= 20250131 AND NOT v_found LOOP
        -- Суммарные простои за текущий день
        SELECT COALESCE(SUM(duration_min) / 60.0, 0) INTO v_daily_hours
        FROM fact_equipment_downtime
        WHERE date_id = v_current_date_id;

        v_cumulative_hours := v_cumulative_hours + v_daily_hours;

        RAISE NOTICE 'Дата: %, Простои: % ч, Накоплено: % ч',
            v_current_date_id,
            ROUND(v_daily_hours, 1),
            ROUND(v_cumulative_hours, 1);

        -- Проверка порога
        IF v_cumulative_hours >= v_threshold THEN
            RAISE NOTICE '';
            RAISE NOTICE 'ПОРГ ДОСТИГНУТ!';
            RAISE NOTICE 'Дата достижения порога: %', v_current_date_id;
            RAISE NOTICE 'Суммарные простои: % ч', ROUND(v_cumulative_hours, 1);
            v_found := TRUE;
            EXIT;
        END IF;

        -- Переход к следующему дню
        v_current_date_id := v_current_date_id + 1;
        CONTINUE WHEN v_current_date_id <= 20250131;
    END LOOP;

    IF NOT v_found THEN
        RAISE NOTICE '';
        RAISE NOTICE 'Порог НЕ достигнут до конца месяца';
        RAISE NOTICE 'Итоговые простои: % ч', ROUND(v_cumulative_hours, 1);
    END IF;
END;
$$;

-- Задание 5. CASE и FOREACH — анализ датчиков (среднее)
DO $$
DECLARE
    v_sensor_type_ids INT[];
    v_sensor_type_id INT;
    v_type_name VARCHAR;
    v_sensor_count INT;
    v_reading_count BIGINT;
    v_status VARCHAR;
BEGIN
    -- Получение массива уникальных sensor_type_id
    SELECT ARRAY_AGG(DISTINCT sensor_type_id) INTO v_sensor_type_ids
    FROM dim_sensor_type;

    RAISE NOTICE 'Анализ датчиков за январь 2025:';
    RAISE NOTICE '--------------------------------';

    -- Перебор типов датчиков
    FOREACH v_sensor_type_id IN ARRAY v_sensor_type_ids LOOP
        -- Название типа датчика
        SELECT type_name INTO v_type_name
        FROM dim_sensor_type
        WHERE sensor_type_id = v_sensor_type_id;

        -- Количество датчиков этого типа
        SELECT COUNT(*) INTO v_sensor_count
        FROM dim_sensor
        WHERE sensor_type_id = v_sensor_type_id;

        -- Количество показаний за январь 2025
        SELECT COUNT(*) INTO v_reading_count
        FROM fact_equipment_telemetry fet
        JOIN dim_sensor ds ON fet.sensor_id = ds.sensor_id
        JOIN dim_date dd ON fet.date_id = dd.date_id
        WHERE ds.sensor_type_id = v_sensor_type_id
          AND dd.year = 2025 AND dd.month = 1;

        -- Определение статуса
        IF v_sensor_count = 0 THEN
            v_status := 'Нет датчиков';
        ELSIF v_reading_count = 0 THEN
            v_status := 'Нет данных';
        ELSE
            CASE
                WHEN v_reading_count / v_sensor_count > 1000 THEN
                    v_status := 'Активно работает';
                WHEN v_reading_count / v_sensor_count BETWEEN 100 AND 1000 THEN
                    v_status := 'Нормальная работа';
                WHEN v_reading_count / v_sensor_count BETWEEN 1 AND 99 THEN
                    v_status := 'Редкие показания';
                ELSE
                    v_status := 'Нет данных';
            END CASE;
        END IF;

        RAISE NOTICE 'Тип: % | Датчиков: % | Показаний: % | Статус: %',
            v_type_name, v_sensor_count, v_reading_count, v_status;
    END LOOP;
END;
$$;

-- Задание 6. Курсор — пакетное формирование отчёта по сменам (среднее)
-- Создание таблицы отчётов в схеме kond
CREATE TABLE IF NOT EXISTS kond.report_shift_summary (
    report_date    DATE,
    shift_name     VARCHAR(50),
    mine_name      VARCHAR(100),
    total_tons     NUMERIC(12,2),
    equipment_used INT,
    efficiency     NUMERIC(5,1),
    created_at     TIMESTAMP DEFAULT NOW()
);

DO $$
DECLARE
    v_date_record RECORD;
    v_shift_record RECORD;
    v_mine_record RECORD;
    v_total_tons NUMERIC;
    v_equipment_count INT;
    v_operating_hours NUMERIC;
    v_efficiency NUMERIC;
    v_inserted_rows INT := 0;
    v_cursor CURSOR FOR
        SELECT DISTINCT d.full_date, d.date_id
        FROM dim_date d
        WHERE d.year = 2025 AND d.month = 1 AND d.day_of_month BETWEEN 1 AND 15
        ORDER BY d.full_date;
BEGIN
    RAISE NOTICE 'Начало формирования отчёта по сменам...';

    -- Открытие курсора
    OPEN v_cursor;

    LOOP
        FETCH v_cursor INTO v_date_record;
        EXIT WHEN NOT FOUND;

        -- Для каждой комбинации смена+шахта
        FOR v_shift_record IN SELECT shift_id, shift_name FROM dim_shift LOOP
            FOR v_mine_record IN SELECT mine_id, mine_name FROM dim_mine LOOP
                -- Агрегированные данные
                SELECT
                    COALESCE(SUM(fp.tons_mined), 0),
                    COUNT(DISTINCT fp.equipment_id),
                    COALESCE(SUM(fp.operating_hours), 0)
                INTO v_total_tons, v_equipment_count, v_operating_hours
                FROM fact_production fp
                WHERE fp.date_id = v_date_record.date_id
                  AND fp.shift_id = v_shift_record.shift_id
                  AND fp.mine_id = v_mine_record.mine_id;

                -- Расчёт эффективности
                IF v_equipment_count > 0 AND v_operating_hours > 0 THEN
                    v_efficiency := (v_operating_hours / (v_equipment_count * 8)) * 100;
                ELSE
                    v_efficiency := 0;
                END IF;

                -- Вставка в таблицу отчётов
                INSERT INTO kond.report_shift_summary (
                    report_date, shift_name, mine_name,
                    total_tons, equipment_used, efficiency
                ) VALUES (
                    v_date_record.full_date,
                    v_shift_record.shift_name,
                    v_mine_record.mine_name,
                    v_total_tons,
                    v_equipment_count,
                    ROUND(v_efficiency, 1)
                );

                v_inserted_rows := v_inserted_rows + 1;
            END LOOP;
        END LOOP;

        RAISE NOTICE 'Обработана дата: %', v_date_record.full_date;
    END LOOP;

    CLOSE v_cursor;

    RAISE NOTICE 'Отчёт сформирован успешно!';
    RAISE NOTICE 'Вставлено строк: %', v_inserted_rows;

    -- Проверка результата
    RAISE NOTICE '';
    RAISE NOTICE 'Проверка результата (первые 10 строк):';
    RAISE NOTICE '--------------------------------------';
END;
$$;

-- Проверка результата задания 6
SELECT * FROM kond.report_shift_summary
ORDER BY report_date, shift_name, mine_name
LIMIT 10;

-- Задание 7. RETURN NEXT — функция генерации отчёта (сложное)
CREATE OR REPLACE FUNCTION kond.get_quality_trend(
    p_year INT,
    p_mine_id INT DEFAULT NULL
)
RETURNS TABLE (
    month_num      INT,
    month_name     VARCHAR,
    samples_count  BIGINT,
    avg_fe         NUMERIC,
    min_fe         NUMERIC,
    max_fe         NUMERIC,
    running_avg_fe NUMERIC,
    trend          VARCHAR
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_month INT;
    v_month_name VARCHAR;
    v_samples_count BIGINT;
    v_avg_fe NUMERIC;
    v_min_fe NUMERIC;
    v_max_fe NUMERIC;
    v_running_total NUMERIC := 0;
    v_running_count INT := 0;
    v_previous_avg NUMERIC := NULL;
    v_current_trend VARCHAR;
BEGIN
    FOR v_month IN 1..12 LOOP
        -- Получение названия месяца
        SELECT d.month_name INTO v_month_name
        FROM dim_date d
        WHERE d.year = p_year AND d.month = v_month
        LIMIT 1;

        -- Статистика по качеству руды
        SELECT
            COUNT(*),
            ROUND(AVG(fq.fe_content)::numeric, 2),
            MIN(fq.fe_content),
            MAX(fq.fe_content)
        INTO v_samples_count, v_avg_fe, v_min_fe, v_max_fe
        FROM fact_ore_quality fq
        JOIN dim_date d ON fq.date_id = d.date_id
        WHERE d.year = p_year AND d.month = v_month
          AND (p_mine_id IS NULL OR fq.mine_id = p_mine_id);

        -- Нарастающее среднее
        IF v_samples_count > 0 THEN
            v_running_total := v_running_total + (v_avg_fe * v_samples_count);
            v_running_count := v_running_count + v_samples_count;
        END IF;

        -- Определение тренда
        IF v_previous_avg IS NULL THEN
            v_current_trend := '—';
        ELSIF v_avg_fe > v_previous_avg + 0.5 THEN
            v_current_trend := 'Улучшение';
        ELSIF v_avg_fe < v_previous_avg - 0.5 THEN
            v_current_trend := 'Ухудшение';
        ELSE
            v_current_trend := 'Стабильно';
        END IF;

        -- Возврат строки
        month_num := v_month;
        month_name := v_month_name;
        samples_count := v_samples_count;
        avg_fe := v_avg_fe;
        min_fe := v_min_fe;
        max_fe := v_max_fe;
        running_avg_fe := CASE WHEN v_running_count > 0
                               THEN ROUND(v_running_total / v_running_count, 2)
                               ELSE 0 END;
        trend := v_current_trend;

        RETURN NEXT;

        -- Сохранение для следующей итерации
        IF v_samples_count > 0 THEN
            v_previous_avg := v_avg_fe;
        END IF;
    END LOOP;

    RETURN;
END;
$$;

-- Тестирование функции get_quality_trend
SELECT * FROM kond.get_quality_trend(2025);
SELECT * FROM kond.get_quality_trend(2025, 1);

-- Задание 8. Комплексная валидация данных (сложное)
CREATE OR REPLACE FUNCTION kond.validate_mes_data(
    p_date_from INT,
    p_date_to   INT
)
RETURNS TABLE (
    check_id      INT,
    check_name    VARCHAR,
    severity      VARCHAR,  -- 'ОШИБКА', 'ПРЕДУПРЕЖДЕНИЕ', 'ИНФО'
    affected_rows BIGINT,
    details       TEXT,
    recommendation TEXT
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_check_id INT := 1;
    v_affected_rows BIGINT;
BEGIN
    -- Проверка 1: Отрицательные значения добычи
    SELECT COUNT(*) INTO v_affected_rows
    FROM fact_production
    WHERE date_id BETWEEN p_date_from AND p_date_to
      AND tons_mined < 0;

    check_id := v_check_id;
    check_name := 'Отрицательные значения добычи';
    severity := 'ОШИБКА';
    affected_rows := v_affected_rows;
    details := format('Найдено %s записей с отрицательной добычей', v_affected_rows);
    recommendation := 'Исправить данные или удалить некорректные записи';
    RETURN NEXT;
    v_check_id := v_check_id + 1;

    -- Проверка 2: Добыча свыше 500 т за одну запись
    SELECT COUNT(*) INTO v_affected_rows
    FROM fact_production
    WHERE date_id BETWEEN p_date_from AND p_date_to
      AND tons_mined > 500;

    check_id := v_check_id;
    check_name := 'Аномально высокая добыча';
    severity := 'ПРЕДУПРЕЖДЕНИЕ';
    affected_rows := v_affected_rows;
    details := format('Найдено %s записей с добычей > 500 т', v_affected_rows);
    recommendation := 'Проверить корректность данных и оборудования';
    RETURN NEXT;
    v_check_id := v_check_id + 1;

    -- Проверка 3: Нулевые рабочие часы при ненулевой добыче
    SELECT COUNT(*) INTO v_affected_rows
    FROM fact_production
    WHERE date_id BETWEEN p_date_from AND p_date_to
      AND operating_hours = 0
      AND tons_mined > 0;

    check_id := v_check_id;
    check_name := 'Добыча при нулевых рабочих часах';
    severity := 'ОШИБКА';
    affected_rows := v_affected_rows;
    details := format('Найдено %s записей с добычей при нулевых рабочих часах', v_affected_rows);
    recommendation := 'Проверить корректность данных о рабочем времени';
    RETURN NEXT;
    v_check_id := v_check_id + 1;

    -- Проверка 4: Рабочие дни без записей о добыче
    WITH working_days AS (
        SELECT DISTINCT date_id
        FROM dim_date
        WHERE date_id BETWEEN p_date_from AND p_date_to
          AND is_weekend = false
          AND is_holiday = false
    )
    SELECT COUNT(*) INTO v_affected_rows
    FROM working_days wd
    WHERE NOT EXISTS (
        SELECT 1 FROM fact_production fp
        WHERE fp.date_id = wd.date_id
    );

    check_id := v_check_id;
    check_name := 'Рабочие дни без записей о добыче';
    severity := 'ПРЕДУПРЕЖДЕНИЕ';
    affected_rows := v_affected_rows;
    details := format('Найдено %s рабочих дней без записей о добыче', v_affected_rows);
    recommendation := 'Проверить систему сбора данных';
    RETURN NEXT;
    v_check_id := v_check_id + 1;

    -- Проверка 5: Содержание Fe вне диапазона 0-100%
    SELECT COUNT(*) INTO v_affected_rows
    FROM fact_ore_quality
    WHERE date_id BETWEEN p_date_from AND p_date_to
      AND (fe_content < 0 OR fe_content > 100);

    check_id := v_check_id;
    check_name := 'Некорректное содержание Fe';
    severity := 'ОШИБКА';
    affected_rows := v_affected_rows;
    details := format('Найдено %s записей с содержанием Fe вне диапазона 0-100%%', v_affected_rows);
    recommendation := 'Проверить лабораторные данные';
    RETURN NEXT;
    v_check_id := v_check_id + 1;

    -- Проверка 6: Простои длительностью > 24 часов
    SELECT COUNT(*) INTO v_affected_rows
    FROM fact_equipment_downtime
    WHERE date_id BETWEEN p_date_from AND p_date_to
      AND duration_min > 1440; -- 24 часа в минутах

    check_id := v_check_id;
    check_name := 'Длительные простои (>24 часов)';
    severity := 'ПРЕДУПРЕЖДЕНИЕ';
    affected_rows := v_affected_rows;
    details := format('Найдено %s записей о простоях длительностью >24 часов', v_affected_rows);
    recommendation := 'Проверить корректность данных о простоях';
    RETURN NEXT;
    v_check_id := v_check_id + 1;

    -- Проверка 7: Оборудование без единой записи о телеметрии
    WITH equipment_with_telemetry AS (
        SELECT DISTINCT equipment_id
        FROM fact_equipment_telemetry
        WHERE date_id BETWEEN p_date_from AND p_date_to
    )
    SELECT COUNT(*) INTO v_affected_rows
    FROM dim_equipment de
    WHERE de.status = 'active'
      AND NOT EXISTS (
          SELECT 1 FROM equipment_with_telemetry ewt
          WHERE ewt.equipment_id = de.equipment_id
      );

    check_id := v_check_id;
    check_name := 'Активное оборудование без телеметрии';
    severity := 'ПРЕДУПРЕЖДЕНИЕ';
    affected_rows := v_affected_rows;
    details := format('Найдено %s единиц активного оборудования без записей телеметрии', v_affected_rows);
    recommendation := 'Проверить работу датчиков и систему сбора телеметрии';
    RETURN NEXT;
    v_check_id := v_check_id + 1;

    -- Проверка 8: Дублирование записей
    WITH duplicates AS (
        SELECT date_id, equipment_id, shift_id, COUNT(*) as cnt
        FROM fact_production
        WHERE date_id BETWEEN p_date_from AND p_date_to
        GROUP BY date_id, equipment_id, shift_id
        HAVING COUNT(*) > 1
    )
    SELECT COUNT(*) INTO v_affected_rows
    FROM duplicates;

    check_id := v_check_id;
    check_name := 'Дублирование записей о добыче';
    severity := 'ОШИБКА';
    affected_rows := v_affected_rows;
    details := format('Найдено %s комбинаций дата+оборудование+смена с дублирующимися записями', v_affected_rows);
    recommendation := 'Удалить дублирующиеся записи или объединить данные';
    RETURN NEXT;

    RETURN;
END;
$$;

-- Тестирование функции validate_mes_data
SELECT * FROM kond.validate_mes_data(20250101, 20250131)
ORDER BY severity DESC, affected_rows DESC;

-- Восстановление search_path к значению по умолчанию
RESET search_path;
