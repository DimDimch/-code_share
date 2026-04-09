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

-- PARAM: t_duplicate_minutes
-- Два entry в одну зону с разницей меньше этого порога = дубль прикладывания, игнорируем.
params AS (
    SELECT 5 AS t_duplicate_minutes
),

-- Все события: добавляем календарную дату по Москве.
-- AT TIME ZONE 'Europe/Moscow' важен: без него 01:00 MSK уедет в предыдущие сутки UTC.
base AS (
    SELECT
        e.site_id,
        e.person_id,
        e.zone_type,
        e.direction,
        e.event_dt_msk                                              AS evt_ts,
        (e.event_dt_msk AT TIME ZONE 'Europe/Moscow')::DATE         AS cal_date
    FROM v_parsecnew_events_enriched e
    WHERE e.person_id IS NOT NULL
      AND e.zone_type  IS NOT NULL
),

-- Помечаем дублирующие entry: тот же сотрудник, та же зона, тот же день,
-- предыдущий entry был менее T_duplicate_minutes назад.
base_with_lag AS (
    SELECT
        b.*,
        LAG(b.evt_ts) OVER (
            PARTITION BY b.site_id, b.person_id, b.zone_type, b.direction, b.cal_date
            ORDER BY b.evt_ts
        ) AS prev_same_dir_ts
    FROM base b
),

-- Убираем дубли entry (< T_duplicate_minutes).
-- Exit дубли не трогаем — пусть остаются, они не навредят.
deduped AS (
    SELECT *
    FROM base_with_lag
    WHERE NOT (
        direction = 'entry'
        AND prev_same_dir_ts IS NOT NULL
        AND EXTRACT(EPOCH FROM (evt_ts - prev_same_dir_ts)) / 60.0
            < (SELECT t_duplicate_minutes FROM params)
    )
),

-- Для каждого entry ищем в рамках ТОГО ЖЕ КАЛЕНДАРНОГО ДНЯ:
--   next_exit  — ближайший exit в той же зоне
--   next_entry — ближайший следующий entry в той же зоне (повторный вход = скрытый выход)
entries AS (
    SELECT
        e.site_id,
        e.person_id,
        e.zone_type,
        e.cal_date,
        e.evt_ts                                                          AS entry_ts,
        MIN(x.evt_ts) FILTER (WHERE x.direction = 'exit')                AS next_exit,
        MIN(x.evt_ts) FILTER (WHERE x.direction = 'entry')               AS next_entry
    FROM deduped e
    LEFT JOIN deduped x
        ON  x.site_id   = e.site_id
        AND x.person_id = e.person_id
        AND x.zone_type = e.zone_type
        AND x.evt_ts    > e.evt_ts
        AND x.cal_date  = e.cal_date   -- только в рамках того же дня
    WHERE e.direction = 'entry'
    GROUP BY e.site_id, e.person_id, e.zone_type, e.cal_date, e.evt_ts
),

-- Формируем сессии из entry
sessions_from_entries AS (
    SELECT
        site_id,
        person_id,
        zone_type,
        cal_date                                                AS session_date,
        entry_ts,
        CASE
            -- Реальный exit есть и он раньше следующего entry (или следующего entry нет)
            WHEN next_exit IS NOT NULL
             AND (next_entry IS NULL OR next_exit <= next_entry)
            THEN next_exit

            -- Следующий entry раньше exit → скрытый выход за 1 секунду до него
            WHEN next_entry IS NOT NULL
             AND (next_exit IS NULL OR next_entry < next_exit)
            THEN next_entry - INTERVAL '1 second'

            -- Нет ни exit ни следующего entry → фантомное присутствие, конец дня
            ELSE (cal_date::TIMESTAMP AT TIME ZONE 'Europe/Moscow'
                  + INTERVAL '1 day' - INTERVAL '1 second')
        END                                                     AS exit_ts,
        CASE
            WHEN next_exit IS NOT NULL
             AND (next_entry IS NULL OR next_exit <= next_entry)
            THEN NULL

            WHEN next_entry IS NOT NULL
             AND (next_exit IS NULL OR next_entry < next_exit)
            THEN 'скрытый выход'

            ELSE 'проставлено до конца дня'
        END                                                     AS comment,
        CASE
            WHEN next_exit IS NOT NULL
             AND (next_entry IS NULL OR next_exit <= next_entry)
            THEN 'HIGH'
            ELSE 'LOW'
        END                                                     AS confidence
    FROM entries
),

-- Orphan exits: exit без предшествующего entry в тот же день
-- → сессия от начала дня (00:00:00 MSK) до этого exit
orphan_exits AS (
    SELECT
        e.site_id,
        e.person_id,
        e.zone_type,
        e.cal_date                                              AS session_date,
        (e.cal_date::TIMESTAMP AT TIME ZONE 'Europe/Moscow')   AS entry_ts,   -- 00:00:00 MSK
        e.evt_ts                                                AS exit_ts,
        'проставлено от начала дня'                             AS comment,
        'LOW'                                                   AS confidence
    FROM deduped e
    WHERE e.direction = 'exit'
      AND NOT EXISTS (
          SELECT 1 FROM deduped e2
          WHERE e2.site_id   = e.site_id
            AND e2.person_id = e.person_id
            AND e2.zone_type = e.zone_type
            AND e2.direction = 'entry'
            AND e2.cal_date  = e.cal_date
            AND e2.evt_ts    < e.evt_ts
      )
)

SELECT
    s.site_id,
    s.person_id,
    s.zone_type,
    s.session_date,
    s.entry_ts,
    s.exit_ts,
    ROUND(EXTRACT(EPOCH FROM (s.exit_ts - s.entry_ts)) / 60.0, 1) AS duration_minutes,
    s.comment,
    s.confidence
FROM (
    SELECT * FROM sessions_from_entries
    UNION ALL
    SELECT * FROM orphan_exits
) s
WHERE s.exit_ts > s.entry_ts;


-- =============================================================================
-- 3. Итоговая витрина: сотрудник — дата — зона — время пребывания
--
-- Многодневные сессии разбиваются по календарным суткам:
-- для каждого дня считается только та часть сессии, которая в него попадает.
--
-- Граница суток = операционный день (PARAM: operational_day_start = 6 часов).
-- День D: [D 06:00, D+1 06:00)
--
-- Формат выгрузки:
--   person_id, full_name, tab_number, department_name,
--   report_date, zone_type,
--   total_minutes, total_hours,
--   first_entry, last_exit,
--   has_open_end, has_open_start, comment, confidence
-- =============================================================================

CREATE OR REPLACE VIEW v_parsecnew_presence_march AS
WITH

-- PARAM: operational_day_start = 0
-- Ряд операционных дней марта 2026.
-- Операционный день D: [D 00:00, D+1 00:00)
march_days AS (
    SELECT gs::DATE AS op_date
    FROM generate_series(
        '2026-03-01'::DATE,
        '2026-03-31'::DATE,
        INTERVAL '1 day'
    ) gs
),

-- Разбиваем каждую сессию по календарным суткам.
-- Для каждого дня берём пересечение сессии с окном [op_date 00:00, op_date+1 00:00).
-- PARAM: operational_day_start — сейчас 0 (календарные сутки).
-- Чтобы переключить на 06:00 — замени 0 на 6 в четырёх местах ниже.
sliced AS (
    SELECT
        s.site_id,
        s.person_id,
        s.zone_type,
        s.comment,
        s.confidence,
        d.op_date                                        AS report_date,
        -- Начало пересечения: максимум из (начала сессии, начала суток)
        GREATEST(
            s.entry_ts,
            (d.op_date + 0 * INTERVAL '1 hour')
        )                                                AS slice_entry,
        -- Конец пересечения: минимум из (конца сессии, конца суток)
        LEAST(
            s.exit_ts,
            (d.op_date + INTERVAL '1 day' + 0 * INTERVAL '1 hour')
        )                                                AS slice_exit
    FROM v_parsecnew_sessions_raw s
    JOIN march_days d
      ON s.entry_ts < (d.op_date + INTERVAL '1 day' + 0 * INTERVAL '1 hour')
     AND s.exit_ts  > (d.op_date + 0 * INTERVAL '1 hour')
    WHERE s.site_id IS NOT NULL
)

SELECT
    sl.site_id,
    sl.person_id,
    COALESCE(p.last_name, '') || ' ' ||
    COALESCE(p.first_name, '') || ' ' ||
    COALESCE(p.middle_name, '')                          AS full_name,
    p.tab_number,
    p.department_name,
    sl.report_date,
    sl.zone_type,
    -- Суммарное время в зоне за операционный день, минуты
    ROUND(
        SUM(EXTRACT(EPOCH FROM (sl.slice_exit - sl.slice_entry)) / 60.0),
        1
    )                                                    AS total_minutes,
    -- То же в часах
    ROUND(
        SUM(EXTRACT(EPOCH FROM (sl.slice_exit - sl.slice_entry)) / 3600.0),
        2
    )                                                    AS total_hours,
    -- Первый вход и последний выход в рамках этого дня
    MIN(sl.slice_entry)                                  AS first_entry,
    MAX(sl.slice_exit)                                   AS last_exit,
    COUNT(*)                                             AS sessions_count,
    -- Флаги ночных смен (наследуем из sessions_raw)
    BOOL_OR(sl.comment = 'проставлено до конца дня')    AS has_open_end,
    BOOL_OR(sl.comment = 'проставлено от начала дня')   AS has_open_start,
    NULLIF(
        STRING_AGG(DISTINCT sl.comment, '; ')
        FILTER (WHERE sl.comment IS NOT NULL),
        ''
    )                                                    AS comment,
    CASE
        WHEN BOOL_OR(sl.confidence = 'LOW') THEN 'LOW'
        ELSE 'HIGH'
    END                                                  AS confidence
FROM sliced sl
LEFT JOIN parsecnew_persons p
    ON  p.site_id   = sl.site_id
    AND p.person_id = sl.person_id
-- Только строки с положительной длительностью
WHERE sl.slice_exit > sl.slice_entry
GROUP BY
    sl.site_id,
    sl.person_id,
    p.last_name,
    p.first_name,
    p.middle_name,
    p.tab_number,
    p.department_name,
    sl.report_date,
    sl.zone_type
ORDER BY
    sl.site_id,
    sl.report_date,
    full_name,
    sl.zone_type;


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
