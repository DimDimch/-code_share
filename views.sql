-- =============================================================================
-- views.sql — аналитические представления для пилота СКУД
--
-- Порядок создания важен (зависимости):
--   1. v_parsecnew_events_enriched   — события + ФИО + департамент
--   2. v_parsecnew_sessions_raw      — сырые сессии (пары вход/выход)
--   3. v_parsecnew_presence_march    — итоговая витрина: сотрудник-дата-зона-время
--
-- Параметры, которые можно менять:
--   Ищи метки -- PARAM: <название> перед нужным выражением.
--
-- Все вьюхи CREATE OR REPLACE — безопасно перезапускать.
-- =============================================================================


-- =============================================================================
-- 1. Обогащённые события: добавляем ФИО и подразделение из справочника
-- =============================================================================

CREATE OR REPLACE VIEW v_parsecnew_events_enriched AS
SELECT
    e.site_id,
    e.event_guid,
    e.event_dt,
    e.event_dt_msk,
    e.transaction_type_id,
    e.direction,         -- 'entry' / 'exit'
    e.person_id,
    e.territory_id,
    e.territory_name,
    e.zone_type,         -- 'territory' / 'building' / 'production'
    e.access_point_type, -- 'turnstile' / 'door'
    e.card_code,
    -- ФИО
    p.last_name,
    p.first_name,
    p.middle_name,
    COALESCE(p.last_name, '') || ' ' ||
    COALESCE(p.first_name, '') || ' ' ||
    COALESCE(p.middle_name, '')         AS full_name,
    p.tab_number,
    p.department_name
FROM parsecnew_events e
LEFT JOIN parsecnew_persons p
    ON p.site_id   = e.site_id
   AND p.person_id = e.person_id;


-- =============================================================================
-- 2. Сырые сессии присутствия в зонах
--
-- Логика:
--   Для каждого события entry — находим ближайшее следующее exit
--   того же сотрудника в той же зоне.
--   Это даёт «чистую» пару вход/выход без восстановления пропущенных событий
--   (gap inference пока за скопом пилота, делается здесь потом).
--
-- Обработка ночных смен (граница суток):
--   Дата сессии определяется через «операционный день».
--   -- PARAM: operational_day_start
--   Сейчас: 6 часов (местное время). Операционный день = 06:00 → 05:59 сл. дня.
--   Чтобы переключиться на календарные сутки (00:00) — замени 6 на 0.
--
-- Незакрытые входы (нет парного exit до конца дня):
--   exit_time = конец операционного дня (05:59:59 следующего дня или 23:59:59)
--   comment   = 'проставлено до конца дня'
--
-- Первое событие дня — exit (ночная смена, вход был вчера):
--   entry_time = начало операционного дня (06:00:00 или 00:00:00)
--   comment    = 'проставлено от начала дня'
-- =============================================================================

CREATE OR REPLACE VIEW v_parsecnew_sessions_raw AS
WITH

-- PARAM: operational_day_start
-- Значение 6 = операционный день начинается в 06:00 MSK.
-- Чтобы использовать календарные сутки — замени 6 на 0 в двух местах ниже.
params AS (
    SELECT
        6 AS op_day_start_hour   -- час начала операционного дня (местное время)
),

-- Все события нашей площадки с person_id (без анонимных)
base AS (
    SELECT
        e.site_id,
        e.person_id,
        e.zone_type,
        e.direction,
        e.event_dt_msk                        AS evt_ts,
        -- Операционная дата: если час события < op_day_start_hour, относим к предыдущему дню
        -- PARAM: operational_day_start — второй вхождение
        CASE
            WHEN EXTRACT(HOUR FROM e.event_dt_msk) < p.op_day_start_hour
            THEN (e.event_dt_msk::DATE - INTERVAL '1 day')::DATE
            ELSE e.event_dt_msk::DATE
        END                                   AS op_date,
        e.event_guid,
        e.access_point_type
    FROM v_parsecnew_events_enriched e
    CROSS JOIN params p
    WHERE e.person_id IS NOT NULL
      AND e.zone_type  IS NOT NULL
),

-- Для каждого entry — ищем ближайший следующий exit в той же зоне
entries AS (
    SELECT
        b.site_id,
        b.person_id,
        b.zone_type,
        b.op_date,
        b.evt_ts     AS entry_ts,
        b.event_guid AS entry_guid,
        -- Ближайший exit после этого entry (по времени, та же зона)
        MIN(x.evt_ts) AS exit_ts_raw
    FROM base b
    LEFT JOIN base x
        ON  x.site_id   = b.site_id
        AND x.person_id = b.person_id
        AND x.zone_type = b.zone_type
        AND x.direction = 'exit'
        AND x.evt_ts    > b.evt_ts
    WHERE b.direction = 'entry'
    GROUP BY
        b.site_id, b.person_id, b.zone_type,
        b.op_date, b.evt_ts, b.event_guid
),

-- «Первые события дня» — exit без предшествующего entry в том же операционном дне
-- (человек зашёл до операционного старта — ночная смена с предыдущего дня)
orphan_exits AS (
    SELECT
        b.site_id,
        b.person_id,
        b.zone_type,
        b.op_date,
        b.evt_ts  AS exit_ts,
        b.event_guid
    FROM base b
    WHERE b.direction = 'exit'
      -- нет ни одного entry в ту же зону в тот же операционный день ДО этого exit
      AND NOT EXISTS (
          SELECT 1
          FROM base e2
          WHERE e2.site_id   = b.site_id
            AND e2.person_id = b.person_id
            AND e2.zone_type = b.zone_type
            AND e2.op_date   = b.op_date
            AND e2.direction = 'entry'
            AND e2.evt_ts    < b.evt_ts
      )
),

-- Сборка итоговых сессий
sessions AS (

    -- Обычные сессии: entry + (exit или конец дня)
    SELECT
        e.site_id,
        e.person_id,
        e.zone_type,
        e.op_date                            AS session_date,
        e.entry_ts,
        COALESCE(e.exit_ts_raw,
            -- Конец операционного дня: op_date + 1 день, op_day_start_hour - 1 сек
            -- PARAM: operational_day_start — третье вхождение
            (e.op_date + INTERVAL '1 day' +
             (SELECT op_day_start_hour FROM params) * INTERVAL '1 hour'
             - INTERVAL '1 second')
        )                                    AS exit_ts,
        CASE
            WHEN e.exit_ts_raw IS NULL THEN 'проставлено до конца дня'
            ELSE NULL
        END                                  AS comment,
        -- Качество: оба реальные / один восстановлен
        CASE
            WHEN e.exit_ts_raw IS NOT NULL THEN 'HIGH'
            ELSE 'LOW'
        END                                  AS confidence
    FROM entries e

    UNION ALL

    -- Сессии для ночников: exit без entry → начало дня → exit
    SELECT
        ox.site_id,
        ox.person_id,
        ox.zone_type,
        ox.op_date                           AS session_date,
        -- Начало операционного дня
        -- PARAM: operational_day_start — четвёртое вхождение
        (ox.op_date +
         (SELECT op_day_start_hour FROM params) * INTERVAL '1 hour')
                                             AS entry_ts,
        ox.exit_ts,
        'проставлено от начала дня'          AS comment,
        'LOW'                                AS confidence
    FROM orphan_exits ox

)

SELECT
    s.site_id,
    s.person_id,
    s.zone_type,
    s.session_date,
    s.entry_ts,
    s.exit_ts,
    -- Длительность в минутах
    ROUND(
        EXTRACT(EPOCH FROM (s.exit_ts - s.entry_ts)) / 60.0,
        1
    )                                        AS duration_minutes,
    s.comment,
    s.confidence
FROM sessions s
-- Защита от отрицательных и нулевых сессий
WHERE s.exit_ts > s.entry_ts;


-- =============================================================================
-- 3. Итоговая витрина: сотрудник — дата — зона — время пребывания
--
-- Формат выгрузки:
--   person_id, full_name, tab_number, department_name,
--   session_date, zone_type,
--   total_minutes (сумма по всем сессиям зоны за день),
--   first_entry, last_exit,
--   has_night_open (флаг: хотя бы одна сессия закрыта до конца дня),
--   has_night_start (флаг: хотя бы одна сессия открыта от начала дня),
--   comment (агрегированный),
--   confidence_min (худший уровень достоверности за день)
-- =============================================================================

CREATE OR REPLACE VIEW v_parsecnew_presence_march AS
SELECT
    s.site_id,
    s.person_id,
    -- ФИО из справочника
    COALESCE(p.last_name, '') || ' ' ||
    COALESCE(p.first_name, '') || ' ' ||
    COALESCE(p.middle_name, '')                         AS full_name,
    p.tab_number,
    p.department_name,
    s.session_date                                      AS report_date,
    s.zone_type,
    -- Суммарное время в зоне за день, минуты
    SUM(s.duration_minutes)                             AS total_minutes,
    -- Суммарное время в зоне за день, часы (для удобства чтения)
    ROUND(SUM(s.duration_minutes) / 60.0, 2)           AS total_hours,
    -- Первый вход и последний выход за день по зоне
    MIN(s.entry_ts)                                     AS first_entry,
    MAX(s.exit_ts)                                      AS last_exit,
    -- Счётчики сессий
    COUNT(*)                                            AS sessions_count,
    -- Флаги ночных смен
    BOOL_OR(s.comment = 'проставлено до конца дня')    AS has_open_end,
    BOOL_OR(s.comment = 'проставлено от начала дня')   AS has_open_start,
    -- Комментарий — собираем уникальные через string_agg
    NULLIF(
        STRING_AGG(DISTINCT s.comment, '; ')
        FILTER (WHERE s.comment IS NOT NULL),
        ''
    )                                                   AS comment,
    -- Уровень достоверности: берём худший (LOW < HIGH)
    CASE
        WHEN BOOL_OR(s.confidence = 'LOW')  THEN 'LOW'
        ELSE 'HIGH'
    END                                                 AS confidence
FROM v_parsecnew_sessions_raw s
LEFT JOIN parsecnew_persons p
    ON  p.site_id   = s.site_id
    AND p.person_id = s.person_id
-- Только март 2026
WHERE s.session_date >= '2026-03-01'
  AND s.session_date <= '2026-03-31'
GROUP BY
    s.site_id,
    s.person_id,
    p.last_name,
    p.first_name,
    p.middle_name,
    p.tab_number,
    p.department_name,
    s.session_date,
    s.zone_type
ORDER BY
    s.site_id,
    s.session_date,
    full_name,
    s.zone_type;


-- =============================================================================
-- Примеры запросов к витрине
-- =============================================================================

-- Выгрузка для валидации (один сайт):
-- SELECT * FROM v_parsecnew_presence_march WHERE site_id = 'site_a' ORDER BY report_date, full_name, zone_type;

-- Только аномальные строки (ночные смены / неполные данные):
-- SELECT * FROM v_parsecnew_presence_march WHERE comment IS NOT NULL ORDER BY report_date, full_name;

-- Суммарное время по сотруднику и зоне за месяц:
-- SELECT person_id, full_name, zone_type, SUM(total_hours) AS hours_march
-- FROM v_parsecnew_presence_march
-- GROUP BY person_id, full_name, zone_type
-- ORDER BY full_name, zone_type;

-- Средняя загрузка по дням (сколько уникальных людей в зоне):
-- SELECT report_date, zone_type, COUNT(DISTINCT person_id) AS persons
-- FROM v_parsecnew_presence_march
-- GROUP BY report_date, zone_type
-- ORDER BY report_date, zone_type;
