-- Модуль 18: Применение транзакций


SET search_path TO kond, public;

-- Задание 1. BEGIN / COMMIT / ROLLBACK

BEGIN;


INSERT INTO fact_production (production_id, date_id, shift_id, mine_id, equipment_id, tons_mined, operator_id, shaft_id)
VALUES
    (1000001, 20250310, 1, 1, 1, 150.5, 1, 1),
    (1000002, 20250310, 1, 1, 2, 120.3, 2, 1),
    (1000003, 20250310, 1, 1, 3, 135.7, 3, 1),
    (1000004, 20250310, 1, 1, 4, 110.2, 4, 1),
    (1000005, 20250310, 1, 1, 5, 125.8, 5, 1);


SELECT COUNT(*) AS records_in_transaction FROM fact_production WHERE date_id = 20250310 AND shift_id = 1;

COMMIT;


SELECT COUNT(*) AS records_after_commit FROM fact_production WHERE date_id = 20250310 AND shift_id = 1;


BEGIN;


INSERT INTO fact_production (production_id, date_id, shift_id, mine_id, equipment_id, tons_mined, operator_id, shaft_id)
VALUES
    (1000006, 20250310, 2, 1, 1, 140.5, 1, 1),
    (1000007, 20250310, 2, 1, 2, 115.3, 2, 1),
    (1000008, 20250310, 2, 1, 3, 130.7, 3, 1),
    (1000009, 20250310, 2, 1, 4, 105.2, 4, 1),
    (1000010, 20250310, 2, 1, 5, 120.8, 5, 1);


SELECT COUNT(*) AS records_before_rollback FROM fact_production WHERE date_id = 20250310 AND shift_id = 2;

ROLLBACK;


SELECT COUNT(*) AS records_after_rollback FROM fact_production WHERE date_id = 20250310 AND shift_id = 2;

-- Задание 2. SAVEPOINT — частичная загрузка (простое)
BEGIN;


INSERT INTO fact_production (production_id, date_id, shift_id, mine_id, equipment_id, tons_mined, operator_id, shaft_id)
VALUES (1000011, 20250311, 1, 1, 1, 155.5, 1, 1);

SAVEPOINT sp_after_production;


INSERT INTO fact_ore_quality (quality_id, date_id, shift_id, time_id, mine_id, shaft_id, fe_content)
VALUES (1000011, 20250311, 1, 1, 1, 1, 53.7);

SAVEPOINT sp_after_quality;



BEGIN
    INSERT INTO fact_equipment_telemetry (telemetry_id, date_id, time_id, equipment_id, sensor_id, sensor_value)
    VALUES (1000011, 20250311, 1, 1, 99999, 25.5);
EXCEPTION
    WHEN foreign_key_violation THEN
        RAISE NOTICE 'Ошибка FK violation: несуществующий sensor_id';
END;


ROLLBACK TO sp_after_quality;


SELECT
    (SELECT COUNT(*) FROM fact_production WHERE production_id = 1000011) AS production_exists,
    (SELECT COUNT(*) FROM fact_ore_quality WHERE quality_id = 1000011) AS quality_exists,
    (SELECT COUNT(*) FROM fact_equipment_telemetry WHERE telemetry_id = 1000011) AS telemetry_exists;

COMMIT;


SELECT
    'fact_production' AS table_name,
    COUNT(*) AS record_count
FROM fact_production
WHERE production_id = 1000011
UNION ALL
SELECT
    'fact_ore_quality',
    COUNT(*)
FROM fact_ore_quality
WHERE quality_id = 1000011
UNION ALL
SELECT
    'fact_equipment_telemetry',
    COUNT(*)
FROM fact_equipment_telemetry
WHERE telemetry_id = 1000011;

-- Задание 3. ACID на практике (простое)

CREATE TABLE IF NOT EXISTS kond.equipment_balance (
    equipment_id INT PRIMARY KEY,
    balance_tons NUMERIC DEFAULT 0,
    CHECK (balance_tons >= 0)
);


TRUNCATE TABLE kond.equipment_balance;
INSERT INTO kond.equipment_balance VALUES (1, 1000), (2, 500);


SELECT * FROM kond.equipment_balance ORDER BY equipment_id;


BEGIN;

UPDATE kond.equipment_balance SET balance_tons = balance_tons - 200 WHERE equipment_id = 1;
UPDATE kond.equipment_balance SET balance_tons = balance_tons + 200 WHERE equipment_id = 2;

COMMIT;


SELECT * FROM kond.equipment_balance ORDER BY equipment_id;


BEGIN;

UPDATE kond.equipment_balance SET balance_tons = balance_tons - 1500 WHERE equipment_id = 2;
UPDATE kond.equipment_balance SET balance_tons = balance_tons + 1500 WHERE equipment_id = 1;


EXCEPTION
    WHEN check_violation THEN
        RAISE NOTICE 'Ошибка CHECK violation: отрицательный баланс';
        ROLLBACK;


SELECT * FROM kond.equipment_balance ORDER BY equipment_id;

-- Задание 5. Обработка конфликтов блокировок (среднее)
CREATE OR REPLACE FUNCTION kond.safe_update_production(
    p_production_id INT,
    p_new_tons NUMERIC,
    p_timeout_ms INT DEFAULT 5000
)
RETURNS VARCHAR
LANGUAGE plpgsql
AS $$
BEGIN

    EXECUTE format('SET lock_timeout TO %s', p_timeout_ms);

    BEGIN

        UPDATE fact_production
        SET tons_mined = p_new_tons,
            loaded_at = NOW()
        WHERE production_id = p_production_id;

        RETURN 'OK';

    EXCEPTION
        WHEN lock_not_available THEN
            RETURN 'ЗАБЛОКИРОВАНО: попробуйте позже';
        WHEN deadlock_detected THEN
            RETURN 'DEADLOCK: повторите операцию';
        WHEN OTHERS THEN
            RAISE;
    END;
END;
$$;

-- Задание 6. Предотвращение Deadlock (среднее, в парах)

CREATE TABLE IF NOT EXISTS kond.mine_daily_stats (
    mine_id    INT,
    date_id    INT,
    total_tons NUMERIC DEFAULT 0,
    status     VARCHAR(20) DEFAULT 'pending',
    PRIMARY KEY (mine_id, date_id)
);


TRUNCATE TABLE kond.mine_daily_stats;
INSERT INTO kond.mine_daily_stats (mine_id, date_id) VALUES (1, 20250301), (2, 20250301);


CREATE OR REPLACE FUNCTION kond.update_mine_stats(
    p_mine_ids INT[],
    p_date_id INT,
    p_tons NUMERIC[]
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    v_sorted_ids INT[];
    v_index INT;
BEGIN

    v_sorted_ids := ARRAY(SELECT unnest(p_mine_ids) ORDER BY 1);


    FOR v_index IN 1..array_length(v_sorted_ids, 1) LOOP
        PERFORM 1
        FROM kond.mine_daily_stats
        WHERE mine_id = v_sorted_ids[v_index]
          AND date_id = p_date_id
        FOR UPDATE;
    END LOOP;


    FOR v_index IN 1..array_length(p_mine_ids, 1) LOOP
        UPDATE kond.mine_daily_stats
        SET total_tons = total_tons + p_tons[v_index],
            status = 'processed'
        WHERE mine_id = p_mine_ids[v_index]
          AND date_id = p_date_id;
    END LOOP;
END;
$$;

-- Задание 7. Advisory Lock — защита ETL (среднее)

CREATE TABLE IF NOT EXISTS kond.report_daily_production (
    report_date DATE PRIMARY KEY,
    total_tons NUMERIC,
    avg_oee NUMERIC,
    equipment_count INT,
    processed_at TIMESTAMP DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION kond.etl_daily_report(p_date_id INT)
RETURNS VARCHAR
LANGUAGE plpgsql
AS $$
DECLARE
    v_date DATE;
    v_lock_acquired BOOLEAN;
    v_total_tons NUMERIC;
    v_avg_oee NUMERIC;
    v_equipment_count INT;
BEGIN

    SELECT full_date INTO v_date FROM dim_date WHERE date_id = p_date_id;


    v_lock_acquired := pg_try_advisory_lock(p_date_id);

    IF NOT v_lock_acquired THEN
        RETURN 'ETL уже запущен';
    END IF;

    BEGIN

        IF EXISTS (SELECT 1 FROM kond.report_daily_production WHERE report_date = v_date) THEN
            RAISE NOTICE 'Дата % уже обработана', v_date;
            PERFORM pg_advisory_unlock(p_date_id);
            RETURN 'Дата уже обработана';
        END IF;


        PERFORM pg_sleep(5);


        SELECT
            COALESCE(SUM(tons_mined), 0),
            COALESCE(ROUND(AVG(
                CASE WHEN operating_hours > 0 AND equipment_count > 0
                     THEN (operating_hours / (equipment_count * 12)) * 100
                     ELSE 0
                END
            ), 1), 0),
            COUNT(DISTINCT equipment_id)
        INTO v_total_tons, v_avg_oee, v_equipment_count
        FROM (
            SELECT
                fp.equipment_id,
                SUM(fp.tons_mined) as tons_mined,
                SUM(fp.operating_hours) as operating_hours,
                COUNT(DISTINCT fp.equipment_id) as equipment_count
            FROM fact_production fp
            WHERE fp.date_id = p_date_id
            GROUP BY fp.equipment_id
        ) subq;


        INSERT INTO kond.report_daily_production (report_date, total_tons, avg_oee, equipment_count)
        VALUES (v_date, v_total_tons, v_avg_oee, v_equipment_count)
        ON CONFLICT (report_date) DO UPDATE SET
            total_tons = EXCLUDED.total_tons,
            avg_oee = EXCLUDED.avg_oee,
            equipment_count = EXCLUDED.equipment_count,
            processed_at = NOW();


        PERFORM pg_advisory_unlock(p_date_id);

        RETURN format('ETL завершён: %s т, OEE: %s%%, оборудование: %s',
                     v_total_tons, v_avg_oee, v_equipment_count);

    EXCEPTION
        WHEN OTHERS THEN

            PERFORM pg_advisory_unlock(p_date_id);
            RAISE;
    END;
END;
$$;

-- Задание 8. MVCC — наблюдение (среднее)

CREATE TABLE IF NOT EXISTS kond.test_mvcc (
    id   INT PRIMARY KEY,
    data VARCHAR(50)
);


TRUNCATE TABLE kond.test_mvcc;
INSERT INTO kond.test_mvcc VALUES (1, 'версия 1');


SELECT ctid, xmin, xmax, * FROM kond.test_mvcc;


BEGIN;
UPDATE kond.test_mvcc SET data = 'версия 2' WHERE id = 1;
COMMIT;

SELECT ctid, xmin, xmax, * FROM kond.test_mvcc;

BEGIN;
UPDATE kond.test_mvcc SET data = 'версия 3' WHERE id = 1;
COMMIT;

SELECT ctid, xmin, xmax, * FROM kond.test_mvcc;

BEGIN;
UPDATE kond.test_mvcc SET data = 'версия 4' WHERE id = 1;
COMMIT;

SELECT ctid, xmin, xmax, * FROM kond.test_mvcc;


VACUUM kond.test_mvcc;
SELECT ctid, xmin, xmax, * FROM kond.test_mvcc;

-- Задание 9. Процедура с управлением транзакциями (сложное)

CREATE TABLE IF NOT EXISTS kond.report_daily_production_full (
    report_date DATE PRIMARY KEY,
    records_count INT,
    total_tons NUMERIC,
    avg_tons_per_equip NUMERIC,
    processed_at TIMESTAMP DEFAULT NOW()
);

CREATE OR REPLACE PROCEDURE kond.load_monthly_production(
    p_year INT,
    p_month INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_day INT;
    v_date_id INT;
    v_date DATE;
    v_records_count INT;
    v_total_tons NUMERIC;
    v_avg_tons NUMERIC;
    v_days_processed INT := 0;
    v_days_error INT := 0;
    v_total_records INT := 0;
    v_error_detail TEXT;
BEGIN
    RAISE NOTICE 'Начало загрузки данных за %-%', p_year, p_month;


    FOR v_day IN 1..31 LOOP
        BEGIN

            v_date_id := p_year * 10000 + p_month * 100 + v_day;


            SELECT full_date INTO v_date
            FROM dim_date
            WHERE date_id = v_date_id;

            IF v_date IS NULL THEN
                CONTINUE;
            END IF;


            SELECT
                COUNT(*),
                COALESCE(SUM(tons_mined), 0),
                COALESCE(ROUND(AVG(tons_mined)::numeric, 2), 0)
            INTO v_records_count, v_total_tons, v_avg_tons
            FROM fact_production
            WHERE date_id = v_date_id;


            INSERT INTO kond.report_daily_production_full (
                report_date, records_count, total_tons, avg_tons_per_equip
            ) VALUES (
                v_date, v_records_count, v_total_tons, v_avg_tons
            )
            ON CONFLICT (report_date) DO UPDATE SET
                records_count = EXCLUDED.records_count,
                total_tons = EXCLUDED.total_tons,
                avg_tons_per_equip = EXCLUDED.avg_tons_per_equip,
                processed_at = NOW();


            COMMIT;

            v_days_processed := v_days_processed + 1;
            v_total_records := v_total_records + v_records_count;

            RAISE NOTICE 'День % обработан: % записей, % т', v_day, v_records_count, v_total_tons;

        EXCEPTION
            WHEN OTHERS THEN
                v_error_detail := SQLERRM;
                v_days_error := v_days_error + 1;


                RAISE WARNING 'Ошибка при обработке дня %: %', v_day, v_error_detail;


                ROLLBACK;
        END;
    END LOOP;


    RAISE NOTICE '';
    RAISE NOTICE 'Загрузка завершена:';
    RAISE NOTICE 'Дней обработано: %', v_days_processed;
    RAISE NOTICE 'Дней с ошибками: %', v_days_error;
    RAISE NOTICE 'Всего записей: %', v_total_records;
END;
$$;

-- Задание 10. Комплексный кейс: параллельная обработка смен (сложное)

CREATE TABLE IF NOT EXISTS kond.shift_summary (
    date_id     INT,
    shift_id    INT,
    mine_id     INT,
    total_tons  NUMERIC,
    total_trips INT,
    oee_percent NUMERIC,
    updated_by  VARCHAR(50),
    updated_at  TIMESTAMP DEFAULT NOW(),
    version     INT DEFAULT 1,
    PRIMARY KEY (date_id, shift_id, mine_id)
);


CREATE OR REPLACE FUNCTION kond.update_shift_summary(
    p_date_id INT,
    p_shift_id INT,
    p_mine_id INT,
    p_total_tons NUMERIC,
    p_version INT
)
RETURNS VARCHAR
LANGUAGE plpgsql
AS $$
DECLARE
    v_current_version INT;
    v_rows_updated INT;
BEGIN

    UPDATE kond.shift_summary
    SET total_tons = p_total_tons,
        updated_by = CURRENT_USER,
        updated_at = NOW(),
        version = version + 1
    WHERE date_id = p_date_id
      AND shift_id = p_shift_id
      AND mine_id = p_mine_id
      AND version = p_version;

    GET DIAGNOSTICS v_rows_updated = ROW_COUNT;

    IF v_rows_updated = 0 THEN

        SELECT version INTO v_current_version
        FROM kond.shift_summary
        WHERE date_id = p_date_id
          AND shift_id = p_shift_id
          AND mine_id = p_mine_id;

        IF v_current_version IS NULL THEN
            RETURN 'Запись не найдена';
        ELSE
            RETURN 'Данные были изменены другим пользователем';
        END IF;
    END IF;

    RETURN 'OK';
END;
$$;


CREATE OR REPLACE FUNCTION kond.refresh_shift_summary(p_date_id INT)
RETURNS VARCHAR
LANGUAGE plpgsql
AS $$
DECLARE
    v_lock_acquired BOOLEAN;
BEGIN

    v_lock_acquired := pg_try_advisory_xact_lock(p_date_id);

    IF NOT v_lock_acquired THEN
        RETURN 'Пересчёт уже выполняется';
    END IF;


    INSERT INTO kond.shift_summary (
        date_id, shift_id, mine_id, total_tons, total_trips, oee_percent, updated_by
    )
    SELECT
        fp.date_id,
        fp.shift_id,
        fp.mine_id,
        SUM(fp.tons_mined) AS total_tons,
        SUM(fp.trips_count) AS total_trips,
        ROUND(
            CASE WHEN SUM(fp.operating_hours) > 0 AND COUNT(DISTINCT fp.equipment_id) > 0
                 THEN (SUM(fp.operating_hours) / (COUNT(DISTINCT fp.equipment_id) * 12)) * 100
                 ELSE 0
            END, 1
        ) AS oee_percent,
        CURRENT_USER AS updated_by
    FROM fact_production fp
    WHERE fp.date_id = p_date_id
    GROUP BY fp.date_id, fp.shift_id, fp.mine_id
    ON CONFLICT (date_id, shift_id, mine_id) DO UPDATE SET
        total_tons = EXCLUDED.total_tons,
        total_trips = EXCLUDED.total_trips,
        oee_percent = EXCLUDED.oee_percent,
        updated_by = EXCLUDED.updated_by,
        updated_at = NOW(),
        version = shift_summary.version + 1;

    RETURN format('Пересчёт завершён: обновлено %s записей', (SELECT COUNT(*) FROM kond.shift_summary WHERE date_id = p_date_id));
END;
$$;


SELECT kond.refresh_shift_summary(20250115);


SELECT * FROM kond.shift_summary WHERE date_id = 20250115 ORDER BY shift_id, mine_id;


RESET search_path;
