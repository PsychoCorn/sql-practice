-- Схема: kond

-- Установка схемы по умолчанию
SET search_path TO kond, public;

-- Задание 1. Добавление нового оборудования (INSERT — одна строка)
INSERT INTO practice_dim_equipment (
    equipment_id,
    equipment_type_id,
    mine_id,
    equipment_name,
    inventory_number,
    manufacturer,
    model,
    year_manufactured,
    commissioning_date,
    status,
    has_video_recorder,
    has_navigation
) VALUES (
    200,
    2,
    2,
    'Самосвал МоАЗ-7529',
    'INV-TRK-200',
    'МоАЗ',
    '7529',
    2025,
    '2025-03-15',
    'active',
    TRUE,
    TRUE
);

-- Проверка
SELECT * FROM practice_dim_equipment WHERE equipment_id = 200;

-- Задание 2. Массовая вставка операторов (INSERT — несколько строк)
INSERT INTO practice_dim_operator (
    operator_id,
    tab_number,
    last_name,
    first_name,
    middle_name,
    position,
    qualification,
    hire_date,
    mine_id
) VALUES
    (200, 'TAB-200', 'Сидоров', 'Михаил', 'Иванович', 'Машинист ПДМ', '4 разряд', '2025-03-01', 1),
    (201, 'TAB-201', 'Петрова', 'Елена', 'Сергеевна', 'Оператор скипа', '3 разряд', '2025-03-01', 2),
    (202, 'TAB-202', 'Волков', 'Дмитрий', 'Алексеевич', 'Водитель самосвала', '5 разряд', '2025-03-10', 2);

-- Проверка
SELECT * FROM practice_dim_operator WHERE operator_id >= 200 ORDER BY operator_id;

-- Задание 3. Загрузка из staging (INSERT ... SELECT)
-- Проверка количества строк до вставки
SELECT COUNT(*) AS before_count FROM practice_fact_production;

-- Вставка валидированных записей с исключением дубликатов
INSERT INTO practice_fact_production (
    production_id,
    date_id,
    shift_id,
    equipment_id,
    operator_id,
    mine_id,
    shaft_id,
    location_id,
    ore_grade_id,
    tons_mined,
    tons_transported,
    trips_count,
    distance_km,
    operating_hours,
    fuel_consumed_l,
    loaded_at
)
SELECT
    3000 + s.staging_id AS production_id,
    s.date_id,
    s.shift_id,
    s.equipment_id,
    s.operator_id,
    s.mine_id,
    s.shaft_id,
    s.location_id,
    s.ore_grade_id,
    s.tons_mined,
    s.tons_transported,
    s.trips_count,
    s.distance_km,
    s.operating_hours,
    s.fuel_consumed_l,
    s.loaded_at
FROM staging_production s
WHERE s.is_validated = TRUE
AND NOT EXISTS (
    SELECT 1
    FROM practice_fact_production p
    WHERE p.date_id = s.date_id
    AND p.shift_id = s.shift_id
    AND p.equipment_id = s.equipment_id
    AND p.operator_id = s.operator_id
);

-- Проверка количества строк после вставки
SELECT COUNT(*) AS after_count FROM practice_fact_production;

-- Задание 4. INSERT ... RETURNING с логированием
WITH inserted_grade AS (
    INSERT INTO practice_dim_ore_grade (
        ore_grade_id,
        grade_name,
        grade_code,
        fe_content_min,
        fe_content_max,
        description
    ) VALUES (
        300,
        'Экспортный',
        'EXPORT',
        63.00,
        68.00,
        'Руда для экспортных поставок'
    )
    RETURNING ore_grade_id, grade_name, grade_code
)
INSERT INTO practice_equipment_log (
    equipment_id,
    action,
    details,
    changed_at
)
SELECT
    0,
    'INSERT',
    'Добавлен сорт руды: ' || grade_name || ' (' || grade_code || ')',
    NOW()
FROM inserted_grade;

-- Проверка
SELECT * FROM practice_dim_ore_grade WHERE ore_grade_id = 300;
SELECT * FROM practice_equipment_log WHERE action = 'INSERT' ORDER BY changed_at DESC LIMIT 1;

-- Задание 5. Обновление статуса оборудования (UPDATE)
WITH updated_equipment AS (
    UPDATE practice_dim_equipment
    SET status = 'maintenance'
    WHERE mine_id = 1
    AND year_manufactured <= 2018
    RETURNING equipment_id, equipment_name, year_manufactured, status
)
SELECT * FROM updated_equipment;

-- Проверка всех единиц со статусом 'maintenance'
SELECT equipment_id, equipment_name, year_manufactured, status
FROM practice_dim_equipment
WHERE status = 'maintenance'
ORDER BY equipment_id;

-- Задание 6. UPDATE с подзапросом
UPDATE practice_dim_equipment e
SET has_navigation = TRUE
WHERE e.has_navigation = FALSE
AND EXISTS (
    SELECT 1
    FROM public.dim_sensor s
    JOIN public.dim_sensor_type st ON s.sensor_type_id = st.sensor_type_id
    WHERE s.equipment_id = e.equipment_id
    AND s.status = 'active'
    AND st.type_code = 'NAV'
);

-- Проверка
SELECT e.equipment_id, e.equipment_name, e.has_navigation
FROM practice_dim_equipment e
WHERE e.has_navigation = TRUE
ORDER BY e.equipment_id;

-- Задание 7. DELETE с условием и архивированием
WITH deleted_telemetry AS (
    DELETE FROM practice_fact_telemetry
    WHERE is_alarm = TRUE
    AND date_id = (SELECT date_id FROM public.dim_date WHERE full_date = '2024-03-15')
    RETURNING *
)
INSERT INTO practice_archive_telemetry (
    telemetry_id,
    date_id,
    time_id,
    equipment_id,
    sensor_id,
    location_id,
    sensor_value,
    is_alarm,
    quality_flag,
    loaded_at,
    archived_at
)
SELECT
    telemetry_id,
    date_id,
    time_id,
    equipment_id,
    sensor_id,
    location_id,
    sensor_value,
    is_alarm,
    quality_flag,
    loaded_at,
    NOW() AS archived_at
FROM deleted_telemetry;

-- Проверка
SELECT 'practice_fact_telemetry' AS table_name, COUNT(*) AS count
FROM practice_fact_telemetry
WHERE is_alarm = TRUE
AND date_id = (SELECT date_id FROM public.dim_date WHERE full_date = '2024-03-15')
UNION ALL
SELECT 'practice_archive_telemetry' AS table_name, COUNT(*) AS count
FROM practice_archive_telemetry
WHERE date_id = (SELECT date_id FROM public.dim_date WHERE full_date = '2024-03-15');

-- Задание 8. MERGE — синхронизация справочника (PostgreSQL 15+)
MERGE INTO practice_dim_downtime_reason AS target
USING staging_downtime_reasons AS source
ON (target.reason_code = source.reason_code)
WHEN MATCHED THEN
    UPDATE SET
        reason_name = source.reason_name,
        category = source.category,
        description = source.description
WHEN NOT MATCHED THEN
    INSERT (reason_id, reason_code, reason_name, category, description)
    VALUES (
        (SELECT COALESCE(MAX(reason_id), 0) + 1 FROM practice_dim_downtime_reason),
        source.reason_code,
        source.reason_name,
        source.category,
        source.description
    );

-- Проверка
SELECT
    'practice_dim_downtime_reason' AS table_name,
    COUNT(*) AS record_count
FROM practice_dim_downtime_reason
UNION ALL
SELECT
    'staging_downtime_reasons' AS table_name,
    COUNT(*) AS record_count
FROM staging_downtime_reasons;

-- Задание 9. UPSERT — идемпотентная загрузка (INSERT ... ON CONFLICT)
INSERT INTO practice_dim_operator (
    operator_id,
    tab_number,
    last_name,
    first_name,
    middle_name,
    position,
    qualification,
    hire_date,
    mine_id
) VALUES
    (203, 'TAB-200', 'Сидоров', 'Михаил', 'Иванович', 'Старший машинист ПДМ', '5 разряд', '2025-03-01', 1),
    (204, 'TAB-201', 'Петрова', 'Елена', 'Сергеевна', 'Оператор скипа', '4 разряд', '2025-03-01', 2),
    (205, 'TAB-NEW', 'Новиков', 'Алексей', 'Петрович', 'Машинист ПДМ', '3 разряд', '2025-03-15', 1)
ON CONFLICT (tab_number) DO UPDATE SET
    position = EXCLUDED.position,
    qualification = EXCLUDED.qualification;

-- Проверка
SELECT * FROM practice_dim_operator
WHERE tab_number IN ('TAB-200', 'TAB-201', 'TAB-NEW')
ORDER BY tab_number;

-- Задание 10. Комплексный ETL-процесс (транзакция)
BEGIN;

-- 1. INSERT: Загрузка валидированных записей о добыче
INSERT INTO practice_fact_production (
    production_id,
    date_id,
    shift_id,
    equipment_id,
    operator_id,
    mine_id,
    shaft_id,
    location_id,
    ore_grade_id,
    tons_mined,
    tons_transported,
    trips_count,
    distance_km,
    operating_hours,
    fuel_consumed_l,
    loaded_at
)
SELECT
    4000 + s.staging_id AS production_id,
    s.date_id,
    s.shift_id,
    s.equipment_id,
    s.operator_id,
    s.mine_id,
    s.shaft_id,
    s.location_id,
    s.ore_grade_id,
    s.tons_mined,
    s.tons_transported,
    s.trips_count,
    s.distance_km,
    s.operating_hours,
    s.fuel_consumed_l,
    s.loaded_at
FROM staging_production s
WHERE s.is_validated = TRUE
AND NOT EXISTS (
    SELECT 1
    FROM practice_fact_production p
    WHERE p.date_id = s.date_id
    AND p.shift_id = s.shift_id
    AND p.equipment_id = s.equipment_id
    AND p.operator_id = s.operator_id
);

-- 2. UPDATE: Обновление статусов оборудования
WITH updated_status AS (
    UPDATE practice_dim_equipment e
    SET status = s.new_status
    FROM staging_equipment_status s
    WHERE e.inventory_number = s.inventory_number
    AND s.new_status IS NOT NULL
    RETURNING e.equipment_id, e.status AS new_status, s.status AS old_status
)
-- 4. Логирование: Запись информации о каждом обновлённом оборудовании
INSERT INTO practice_equipment_log (
    equipment_id,
    action,
    old_status,
    new_status,
    details,
    changed_at,
    changed_by
)
SELECT
    equipment_id,
    'ETL_UPDATE',
    old_status,
    new_status,
    'Статус обновлён в процессе ETL',
    NOW(),
    'ETL_PROCESS'
FROM updated_status;

-- 3. DELETE + архивирование: Удаление записей с quality_flag = 'ERROR'
WITH deleted_telemetry AS (
    DELETE FROM practice_fact_telemetry
    WHERE quality_flag = 'ERROR'
    RETURNING *
)
INSERT INTO practice_archive_telemetry (
    telemetry_id,
    date_id,
    time_id,
    equipment_id,
    sensor_id,
    location_id,
    sensor_value,
    is_alarm,
    quality_flag,
    loaded_at,
    archived_at
)
SELECT
    telemetry_id,
    date_id,
    time_id,
    equipment_id,
    sensor_id,
    location_id,
    sensor_value,
    is_alarm,
    quality_flag,
    loaded_at,
    NOW() AS archived_at
FROM deleted_telemetry;

-- 5. Очистка staging
TRUNCATE TABLE staging_production;
TRUNCATE TABLE staging_equipment_status;

COMMIT;

-- Проверка после выполнения транзакции
DO $$
DECLARE
    production_count INTEGER;
    log_count INTEGER;
    archive_count INTEGER;
BEGIN
    -- Проверка новых записей в production
    SELECT COUNT(*) INTO production_count
    FROM practice_fact_production
    WHERE production_id >= 4000;

    -- Проверка записей аудита
    SELECT COUNT(*) INTO log_count
    FROM practice_equipment_log
    WHERE action = 'ETL_UPDATE';

    -- Проверка архивных записей
    SELECT COUNT(*) INTO archive_count
    FROM practice_archive_telemetry
    WHERE quality_flag = 'ERROR';

    RAISE NOTICE 'ETL процесс завершён:';
    RAISE NOTICE '  - Новых записей в production: %', production_count;
    RAISE NOTICE '  - Записей аудита: %', log_count;
    RAISE NOTICE '  - Архивных записей: %', archive_count;
    RAISE NOTICE '  - staging_production пуста: %', (SELECT COUNT(*) = 0 FROM staging_production);
    RAISE NOTICE '  - staging_equipment_status пуста: %', (SELECT COUNT(*) = 0 FROM staging_equipment_status);
END $$;

-- Восстановление пути поиска по умолчанию
RESET search_path;
