-- Задание 1. Анализ длины строковых полей
SELECT
    equipment_name,
    LENGTH(equipment_name) AS name_len,
    LENGTH(inventory_number) AS inv_len,
    LENGTH(model) AS model_len,
    LENGTH(manufacturer) AS manuf_len,
    LENGTH(equipment_name) + LENGTH(inventory_number) + LENGTH(model) + LENGTH(manufacturer) AS total_text_length
FROM dim_equipment
ORDER BY total_text_length DESC, equipment_name;

-- Задание 2. Разбор инвентарного номера
SELECT
    equipment_name,
    inventory_number,
    SPLIT_PART(inventory_number, '-', 1) AS prefix,
    SPLIT_PART(inventory_number, '-', 2) AS type_code,
    CAST(SPLIT_PART(inventory_number, '-', 3) AS INTEGER) AS serial_num,
    CASE
        WHEN SPLIT_PART(inventory_number, '-', 2) = 'CRT' THEN 'Вагонетка'
        WHEN SPLIT_PART(inventory_number, '-', 2) = 'LHD' THEN 'Погрузочно-доставочная машина'
        WHEN SPLIT_PART(inventory_number, '-', 2) = 'SKP' THEN 'Скиповой подъёмник'
        WHEN SPLIT_PART(inventory_number, '-', 2) = 'TRK' THEN 'Шахтный самосвал'
        ELSE 'Неизвестный тип'
    END AS type_description
FROM dim_equipment
ORDER BY type_code, serial_num;

-- Задание 3. Формирование краткого имени оператора
SELECT
    last_name,
    first_name,
    middle_name,
    CONCAT(
        last_name, ' ',
        LEFT(first_name, 1), '.',
        CASE
            WHEN middle_name IS NOT NULL THEN LEFT(middle_name, 1) || '.'
            ELSE ''
        END
    ) AS short_name_1,
    CONCAT(
        LEFT(first_name, 1), '.',
        CASE
            WHEN middle_name IS NOT NULL THEN LEFT(middle_name, 1) || '. '
            ELSE ' '
        END,
        last_name
    ) AS short_name_2,
    UPPER(last_name) AS upper_last,
    LOWER(position) AS lower_position
FROM dim_operator
ORDER BY last_name;

-- Задание 4. Поиск оборудования по шаблону

-- 4a. Оборудование с «ПДМ» в названии
SELECT equipment_name, inventory_number
FROM dim_equipment
WHERE equipment_name LIKE '%ПДМ%'
ORDER BY equipment_name;

-- 4b. Производители на «S» (без учёта регистра)
SELECT DISTINCT manufacturer
FROM dim_equipment
WHERE UPPER(manufacturer) LIKE 'S%'
ORDER BY manufacturer;

-- 4c. Шахты с кавычками в названии
SELECT mine_name
FROM dim_mine
WHERE mine_name LIKE '%"%'
ORDER BY mine_name;

-- 4d. Инвентарные номера 001-010 с использованием регулярного выражения
SELECT inventory_number, equipment_name
FROM dim_equipment
WHERE inventory_number ~ 'INV-(CRT|LHD|SKP|TRK)-0(0[1-9]|10)$'
ORDER BY inventory_number;

-- Задание 5. Список оборудования по шахтам
SELECT
    m.mine_name,
    COUNT(e.equipment_id) AS eq_count,
    STRING_AGG(e.equipment_name, ', ' ORDER BY e.equipment_name) AS equipment_list,
    STRING_AGG(DISTINCT e.manufacturer, ', ' ORDER BY e.manufacturer) AS manufacturers
FROM dim_mine m
JOIN dim_equipment e ON m.mine_id = e.mine_id
GROUP BY m.mine_name
ORDER BY m.mine_name;

-- Задание 6. Возраст оборудования
SELECT
    equipment_name,
    commissioning_date,
    AGE(CURRENT_DATE, commissioning_date) AS age_interval,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, commissioning_date)) AS years,
    CURRENT_DATE - commissioning_date AS days,
    CASE
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, commissioning_date)) < 2 THEN 'Новое'
        WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, commissioning_date)) <= 4 THEN 'Рабочее'
        ELSE 'Требует внимания'
    END AS category
FROM dim_equipment
WHERE commissioning_date IS NOT NULL
ORDER BY days DESC;

-- Задание 7. Форматирование дат для отчётов
SELECT
    equipment_name,
    commissioning_date,
    TO_CHAR(commissioning_date, 'DD.MM.YYYY') AS russian_fmt,
    TO_CHAR(commissioning_date, 'DD Month YYYY г.') AS full_fmt,
    TO_CHAR(commissioning_date, 'YYYY-MM-DD') AS iso_fmt,
    TO_CHAR(commissioning_date, 'YYYY-"Q"Q') AS year_quarter,
    TO_CHAR(commissioning_date, 'Day') AS day_name,
    TO_CHAR(commissioning_date, 'YYYY-MM') AS year_month
FROM dim_equipment
WHERE commissioning_date IS NOT NULL
ORDER BY commissioning_date;

-- Задание 8. Анализ простоев по дням недели и часам

-- По дням недели
SELECT
    TO_CHAR(start_time, 'Day') AS day_of_week,
    COUNT(*) AS downtime_count,
    ROUND(AVG(duration_min), 2) AS avg_duration
FROM fact_equipment_downtime
GROUP BY TO_CHAR(start_time, 'Day'), EXTRACT(DOW FROM start_time)
ORDER BY EXTRACT(DOW FROM start_time);

-- По часам (топ-10)
SELECT
    EXTRACT(HOUR FROM start_time) AS hour_of_day,
    COUNT(*) AS downtime_count
FROM fact_equipment_downtime
GROUP BY EXTRACT(HOUR FROM start_time)
ORDER BY downtime_count DESC
LIMIT 10;

-- Пиковый час
SELECT
    EXTRACT(HOUR FROM start_time) AS peak_hour,
    COUNT(*) AS downtime_count
FROM fact_equipment_downtime
GROUP BY EXTRACT(HOUR FROM start_time)
ORDER BY downtime_count DESC
LIMIT 1;

-- Задание 9. Расчёт графика калибровки датчиков
SELECT
    s.sensor_code,
    st.type_name,
    e.equipment_name,
    s.calibration_date,
    CURRENT_DATE - s.calibration_date AS days_since,
    s.calibration_date + INTERVAL '180 days' AS next_calibration,
    CASE
        WHEN CURRENT_DATE > s.calibration_date + INTERVAL '180 days' THEN 'Просрочена'
        WHEN CURRENT_DATE > s.calibration_date + INTERVAL '150 days' THEN 'Скоро'
        ELSE 'В норме'
    END AS cal_status
FROM dim_sensor s
JOIN dim_sensor_type st ON s.sensor_type_id = st.sensor_type_id
JOIN dim_equipment e ON s.equipment_id = e.equipment_id
WHERE s.calibration_date IS NOT NULL
ORDER BY
    CASE
        WHEN CURRENT_DATE > s.calibration_date + INTERVAL '180 days' THEN 1
        WHEN CURRENT_DATE > s.calibration_date + INTERVAL '150 days' THEN 2
        ELSE 3
    END,
    s.calibration_date;

-- Задание 10. Комплексный отчёт: карточка оборудования
SELECT
    CONCAT(
        '[',
        CASE
            WHEN inventory_number LIKE 'INV-CRT-%' THEN 'Вагонетка'
            WHEN inventory_number LIKE 'INV-LHD-%' THEN 'Погрузочно-доставочная машина'
            WHEN inventory_number LIKE 'INV-SKP-%' THEN 'Скиповой подъёмник'
            WHEN inventory_number LIKE 'INV-TRK-%' THEN 'Самосвал'
            ELSE 'Неизвестный тип'
        END,
        '] ',
        equipment_name,
        ' (',
        COALESCE(manufacturer, 'Не указан'),
        CASE WHEN model IS NOT NULL THEN ' ' || model ELSE '' END,
        ') | Шахта: ',
        (SELECT mine_name FROM dim_mine m WHERE m.mine_id = e.mine_id),
        ' | Введён: ',
        TO_CHAR(commissioning_date, 'DD.MM.YYYY'),
        ' | Возраст: ',
        EXTRACT(YEAR FROM AGE(CURRENT_DATE, commissioning_date)),
        ' лет | Статус: ',
        CASE status
            WHEN 'active' THEN 'АКТИВЕН'
            WHEN 'maintenance' THEN 'НА ТО'
            WHEN 'decommissioned' THEN 'СПИСАН'
            ELSE UPPER(status)
        END,
        ' | Видеорег.: ',
        CASE WHEN has_video_recorder THEN 'ДА' ELSE 'НЕТ' END,
        ' | Навигация: ',
        CASE WHEN has_navigation THEN 'ДА' ELSE 'НЕТ' END
    ) AS equipment_card
FROM dim_equipment e
WHERE commissioning_date IS NOT NULL
ORDER BY equipment_name;
