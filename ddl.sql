-- =============================================================================
-- ddl.sql — схема БД для пилота СКУД
-- Запускать вручную перед первым collect.py
-- Все таблицы с префиксом parsecnew_
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Сотрудники
-- -----------------------------------------------------------------------------
-- Источник: GetOrgUnitsHierarhyWithPersons из СКУД.
-- PK = (site_id, person_id) — GUID независимы между инстансами,
-- поэтому без site_id может быть коллизия.
-- department_name — денормализовано (name из OrgUnit на момент сбора),
-- для JOIN с кадровыми витринами используй tab_number или person_id вручную.

CREATE TABLE IF NOT EXISTS parsecnew_persons (
    site_id         TEXT        NOT NULL,   -- ключ из config.SITES, напр. 'site_a'
    person_id       UUID        NOT NULL,   -- ID сотрудника в СКУД
    last_name       TEXT,
    first_name      TEXT,
    middle_name     TEXT,
    tab_number      TEXT,                   -- табельный номер для матчинга с HR
    department_name TEXT,                   -- название подразделения на момент сбора
    synced_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (site_id, person_id)
);

CREATE INDEX IF NOT EXISTS idx_parsecnew_persons_tab
    ON parsecnew_persons (tab_number);

CREATE INDEX IF NOT EXISTS idx_parsecnew_persons_site
    ON parsecnew_persons (site_id);


-- -----------------------------------------------------------------------------
-- События
-- -----------------------------------------------------------------------------
-- Источник: OpenEventHistorySession / GetEventHistoryResult из СКУД.
-- Только типы транзакций из config.COLLECT.transaction_types (входы/выходы).
-- Только territory_name, присутствующие в config.SITES[site].zones (все зоны).
--
-- zone_type — проставляется Python при вставке по конфигу зон:
--   'territory'  — КПП / периметр
--   'building'   — вход в здание
--   'production' — производственная зона
--   NULL         — territory_name не входит ни в одну зону (не должно быть
--                  в сырых данных, т.к. фильтруем при сборе)
--
-- direction — определяется по transaction_type_id:
--   'entry' — 590144, 590152
--   'exit'  — 590145, 590153, 590146, 590244, 590245
--
-- event_dt     — UTC (как отдаёт СКУД)
-- event_dt_msk — UTC+3 (Москва), удобно для отчётов
--
-- access_point_type — тип точки прохода из конфига:
--   'turnstile' — турникет (1 прикладывание = 1 человек)
--   'door'      — дверь (1 прикладывание ≠ 1 человек)

CREATE TABLE IF NOT EXISTS parsecnew_events (
    site_id              TEXT        NOT NULL,
    event_guid           TEXT        NOT NULL,   -- GUID события из СКУД
    event_dt             TIMESTAMPTZ NOT NULL,   -- время события, UTC
    event_dt_msk         TIMESTAMPTZ NOT NULL,   -- время события, UTC+3
    transaction_type_id  INTEGER     NOT NULL,
    direction            TEXT        NOT NULL CHECK (direction IN ('entry', 'exit')),
    person_id            UUID,                   -- NULL если событие без сотрудника
    territory_id         UUID,
    territory_name       TEXT,
    zone_type            TEXT        CHECK (zone_type IN ('territory', 'building', 'production')),
    access_point_type    TEXT        CHECK (access_point_type IN ('turnstile', 'door')),
    card_code            TEXT,
    PRIMARY KEY (site_id, event_guid)
);

CREATE INDEX IF NOT EXISTS idx_parsecnew_events_dt
    ON parsecnew_events (event_dt);

CREATE INDEX IF NOT EXISTS idx_parsecnew_events_dt_msk
    ON parsecnew_events (event_dt_msk);

CREATE INDEX IF NOT EXISTS idx_parsecnew_events_person
    ON parsecnew_events (site_id, person_id);

CREATE INDEX IF NOT EXISTS idx_parsecnew_events_zone
    ON parsecnew_events (site_id, zone_type, direction);

CREATE INDEX IF NOT EXISTS idx_parsecnew_events_territory
    ON parsecnew_events (site_id, territory_id);
