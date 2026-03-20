# %% [markdown]
# # Parsec ACS — два инстанса, PostgreSQL
# httpx + lxml + psycopg2, без f-строк

# %% [markdown]
# ## 0. Зависимости
# pip install httpx lxml psycopg2-binary

# %% [markdown]
# ## 1. Конфигурация

# %%
from datetime import datetime, timezone, timedelta
import psycopg2
import psycopg2.extras

# URL подключения к PostgreSQL
PG_URL = "postgresql://user:password@localhost:5432/parsec"

# Два инстанса Parsec
# source_id — короткий уникальный идентификатор, используется как FK везде
SOURCES = [
    {
        "source_id":    "msk",
        "city":         "Москва",
        "host_addr":    "192.168.1.50",
        "organization": "SYSTEM",
        "username":     "parsec",
        "password":     "parsec",
        "timeout":      60,
    },
    {
        "source_id":    "spb",
        "city":         "Санкт-Петербург",
        "host_addr":    "10.0.1.50",
        "organization": "SYSTEM",
        "username":     "parsec",
        "password":     "parsec",
        "timeout":      60,
    },
]

HISTORY_START     = datetime(2022, 1, 1, tzinfo=timezone.utc)
TRANSACTION_TYPES = [590144, 590145, 590152, 590153, 590146, 590244, 590245]
MAX_RESULT_SIZE   = 10_000_000
CHUNK_SIZE        = 500

NS_PARSEC = "http://parsec.ru/Parsec3IntergationService"
NS_SOAP   = "http://schemas.xmlsoap.org/soap/envelope/"

# Минимальный набор полей событий
EVENT_FIELD_GUIDS = [
    "9f7a30e6-c9ed-4e62-83e3-59032a0f8d27",  # [0] event_guid
    "2c5ee108-28e3-4dcc-8c95-7f3222d8e67f",  # [1] event_dt
    "57ca38e4-ed6f-4d12-adcb-2faa16f950d7",  # [2] transaction_type_id
    "7c6d82a0-c8c8-495b-9728-357807193d23",  # [3] person_id
    "42dab9c6-5d30-4030-8ccd-2cad6fcbc5f2",  # [4] territory_ids
    "0de358e0-c91b-4333-b902-000000000005",  # [5] card_code
]

print("Конфигурация загружена. Инстансов: " + str(len(SOURCES)))


# %% [markdown]
# ## 2. Схема БД

# %%

SCHEMA = """
-- Справочник источников (инстансов Parsec)
CREATE TABLE IF NOT EXISTS dim_sources (
    source_id   TEXT PRIMARY KEY,   -- 'msk', 'spb', ...
    city        TEXT,
    host_addr   TEXT,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- ── Факты ─────────────────────────────────────────────────────────────────────
-- PK = (source_id, event_guid) — GUID-ы независимы между инстансами
CREATE TABLE IF NOT EXISTS events (
    source_id           TEXT        NOT NULL REFERENCES dim_sources(source_id),
    event_guid          TEXT        NOT NULL,
    event_dt            TIMESTAMPTZ NOT NULL,
    transaction_type_id INTEGER,
    person_id           UUID,
    territory_id        UUID,
    card_code           TEXT,
    PRIMARY KEY (source_id, event_guid)
);
CREATE INDEX IF NOT EXISTS idx_events_dt        ON events(event_dt);
CREATE INDEX IF NOT EXISTS idx_events_person    ON events(person_id);
CREATE INDEX IF NOT EXISTS idx_events_territory ON events(source_id, territory_id);
CREATE INDEX IF NOT EXISTS idx_events_type      ON events(transaction_type_id);
CREATE INDEX IF NOT EXISTS idx_events_source    ON events(source_id);

-- ── Справочники ───────────────────────────────────────────────────────────────

-- Сотрудники
-- PK = (source_id, person_id) — не уверены что GUID-ы одинаковые между городами.
-- canonical_person_id — поле для ручного/автоматического склеивания:
--   если один человек есть в обоих инстансах, сюда пишем общий UUID
--   и джойним аналитику по нему, а не по person_id
CREATE TABLE IF NOT EXISTS dim_persons (
    source_id          TEXT NOT NULL REFERENCES dim_sources(source_id),
    person_id          UUID NOT NULL,
    last_name          TEXT,
    first_name         TEXT,
    middle_name        TEXT,
    tab_number         TEXT,
    org_id             UUID,
    canonical_person_id UUID,   -- NULL = не сматчен, заполняется вручную или скриптом
    synced_at          TIMESTAMPTZ,
    PRIMARY KEY (source_id, person_id)
);
CREATE INDEX IF NOT EXISTS idx_persons_canonical ON dim_persons(canonical_person_id);
CREATE INDEX IF NOT EXISTS idx_persons_tab       ON dim_persons(tab_number);

-- Подразделения — у каждого города своя иерархия
CREATE TABLE IF NOT EXISTS dim_org_units (
    source_id TEXT NOT NULL REFERENCES dim_sources(source_id),
    id        UUID NOT NULL,
    name      TEXT,
    descr     TEXT,
    parent_id UUID,
    PRIMARY KEY (source_id, id)
);

-- Территории (точки прохода) — у каждого города свои
CREATE TABLE IF NOT EXISTS dim_territories (
    source_id TEXT    NOT NULL REFERENCES dim_sources(source_id),
    id        UUID    NOT NULL,
    type      INTEGER,          -- 0=папка, 1=дверь, 3=другое
    name      TEXT,
    descr     TEXT,
    parent_id UUID,
    PRIMARY KEY (source_id, id)
);

-- Типы транзакций — системные, одинаковые для всех инстансов
CREATE TABLE IF NOT EXISTS dim_transaction_types (
    id         INTEGER PRIMARY KEY,
    name       TEXT,
    class_mask BIGINT
);

-- Прогресс загрузки — отдельно на каждый инстанс
CREATE TABLE IF NOT EXISTS _sync_log (
    source_id    TEXT        NOT NULL REFERENCES dim_sources(source_id),
    period_start TIMESTAMPTZ NOT NULL,
    period_end   TIMESTAMPTZ,
    status       TEXT DEFAULT 'pending',  -- pending | running | done | error
    rows_loaded  INTEGER DEFAULT 0,
    started_at   TIMESTAMPTZ,
    finished_at  TIMESTAMPTZ,
    error_msg    TEXT,
    PRIMARY KEY (source_id, period_start)
);
"""

# ── Представления для удобной аналитики ──────────────────────────────────────
# Создаём отдельно — не ломают init_db при повторном запуске

VIEWS = """
-- Полная таблица событий с расшифровкой (для Power BI / dbt)
CREATE OR REPLACE VIEW v_events AS
SELECT
    e.source_id,
    s.city,
    e.event_guid,
    e.event_dt,
    e.event_dt AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Moscow' AS event_dt_local,
    e.transaction_type_id,
    tt.name                                         AS transaction_name,
    -- вход или выход
    CASE
        WHEN e.transaction_type_id IN (590144, 590152) THEN 'entry'
        WHEN e.transaction_type_id IN (590145, 590153, 590146, 590244, 590245) THEN 'exit'
        ELSE 'other'
    END                                             AS event_direction,
    e.person_id,
    -- canonical_person_id для сквозной аналитики по двум городам
    p.canonical_person_id,
    COALESCE(p.canonical_person_id, e.person_id)   AS person_key,
    p.last_name,
    p.first_name,
    p.middle_name,
    p.tab_number,
    p.org_id,
    o.name                                          AS department,
    e.territory_id,
    t.name                                          AS territory_name,
    t.type                                          AS territory_type,
    e.card_code
FROM events e
LEFT JOIN dim_sources           s  ON s.source_id  = e.source_id
LEFT JOIN dim_transaction_types tt ON tt.id        = e.transaction_type_id
LEFT JOIN dim_persons           p  ON p.source_id  = e.source_id
                                   AND p.person_id = e.person_id
LEFT JOIN dim_org_units         o  ON o.source_id  = e.source_id
                                   AND o.id        = p.org_id
LEFT JOIN dim_territories       t  ON t.source_id  = e.source_id
                                   AND t.id        = e.territory_id;
"""


def pg_connect(pg_url=PG_URL):
    con = psycopg2.connect(pg_url)
    con.autocommit = False
    return con


def init_db(pg_url=PG_URL):
    con = pg_connect(pg_url)
    cur = con.cursor()
    cur.execute(SCHEMA)
    cur.execute(VIEWS)
    # Upsert источников из конфига
    for src in SOURCES:
        cur.execute(
            """INSERT INTO dim_sources(source_id, city, host_addr)
               VALUES(%s, %s, %s)
               ON CONFLICT(source_id) DO UPDATE SET
                   city=EXCLUDED.city, host_addr=EXCLUDED.host_addr""",
            (src["source_id"], src["city"], src["host_addr"])
        )
    con.commit()
    cur.close()
    print("БД инициализирована.")
    return con


def _to_uuid(val):
    if not val:
        return None
    val = val.strip()
    return val if (len(val) == 36 and val.count("-") == 4) else None


def _to_int(val):
    try:    return int(val)
    except: return None


print("Схема и init_db определены.")


# %% [markdown]
# ## 3. SOAP-клиент

# %%
import httpx
from lxml import etree


def _envelope(method, body_inner):
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        + '<soap:Envelope'
        + ' xmlns:soap="' + NS_SOAP + '"'
        + ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
        + ' xmlns:xsd="http://www.w3.org/2001/XMLSchema">'
        + '<soap:Body>'
        + '<' + method + ' xmlns="' + NS_PARSEC + '">'
        + body_inner.strip()
        + '</' + method + '>'
        + '</soap:Body>'
        + '</soap:Envelope>'
    ).encode("utf-8")


def _parse(response_bytes):
    root  = etree.fromstring(response_bytes)
    body  = root.find("{" + NS_SOAP + "}Body")
    if body is None:
        raise ValueError("SOAP Body не найден")
    fault = body.find("{" + NS_SOAP + "}Fault")
    if fault is not None:
        raise RuntimeError("SOAP Fault: " + (_text(fault, "faultstring") or ""))
    return body


def _text(node, tag, default=None):
    for child in node:
        if etree.QName(child.tag).localname == tag:
            return (child.text or "").strip() or default
    return default


def _child(node, *names):
    cur = node
    for name in names:
        found = None
        for ch in cur:
            if etree.QName(ch.tag).localname == name:
                found = ch
                break
        if found is None:
            return None
        cur = found
    return cur


class ParsecClient:

    def __init__(self, source_cfg):
        self.source_id  = source_cfg["source_id"]
        self.config     = source_cfg
        self.base_url   = (
            "http://" + source_cfg["host_addr"]
            + ":10101/IntegrationService/IntegrationService.asmx"
        )
        self._http      = None
        self.session_id = None

    def _post(self, method, body_xml):
        payload = _envelope(method, body_xml)
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction":   '"' + NS_PARSEC + "/" + method + '"',
        }
        resp = self._http.post(self.base_url, content=payload, headers=headers)
        resp.raise_for_status()
        return _parse(resp.content)

    def connect(self):
        self._http = httpx.Client(timeout=self.config.get("timeout", 60))
        body = (
            "<domain>"    + self.config["organization"] + "</domain>"
            + "<userName>" + self.config["username"]     + "</userName>"
            + "<password>" + self.config["password"]     + "</password>"
        )
        body_el = self._post("OpenSession", body)
        result  = _child(body_el, "OpenSessionResponse", "OpenSessionResult")
        if result is None:
            raise RuntimeError("OpenSession: неожиданная структура ответа")
        if _text(result, "Result") == "-1":
            raise ConnectionError(
                "[" + self.source_id + "] Авторизация: "
                + (_text(result, "ErrorMessage") or "")
            )
        self.session_id = _text(_child(result, "Value"), "SessionID")
        print("[" + self.source_id + "] Сессия открыта: " + self.session_id)
        return self

    def disconnect(self):
        if self.session_id and self._http:
            try:
                self._post("CloseSession",
                           "<sessionID>" + self.session_id + "</sessionID>")
                print("[" + self.source_id + "] Сессия закрыта.")
            except Exception as e:
                print("[" + self.source_id + "] Предупреждение: " + str(e))
        if self._http:
            self._http.close()
        self.session_id = None
        self._http      = None

    def __enter__(self):
        return self.connect()

    def __exit__(self, *_):
        self.disconnect()

    def call(self, method, extra=""):
        body    = "<sessionID>" + self.session_id + "</sessionID>\n" + extra
        body_el = self._post(method, body)
        return _child(body_el, method + "Response")


def _guid_result(resp_el, method):
    result_el = _child(resp_el, method + "Result")
    if result_el is None:
        raise RuntimeError(method + "Result не найден")
    if _text(result_el, "Result") == "-1":
        raise RuntimeError(method + ": " + (_text(result_el, "ErrorMessage") or ""))
    return _text(result_el, "Value")


print("ParsecClient определён.")


# %% [markdown]
# ## 4. Справочники

# %%

def sync_org_units(c, con):
    print("[" + c.source_id + "] dim_org_units...")
    resp      = c.call("GetOrgUnitsHierarhy")
    result_el = _child(resp, "GetOrgUnitsHierarhyResult")
    if result_el is None:
        return 0
    rows = [
        (c.source_id, _to_uuid(_text(u, "ID")), _text(u, "NAME"),
         _text(u, "DESC"), _to_uuid(_text(u, "PARENT_ID")))
        for u in result_el
    ]
    cur = con.cursor()
    psycopg2.extras.execute_batch(
        cur,
        """INSERT INTO dim_org_units(source_id, id, name, descr, parent_id)
           VALUES(%s, %s, %s, %s, %s)
           ON CONFLICT(source_id, id) DO UPDATE SET
               name=EXCLUDED.name, descr=EXCLUDED.descr, parent_id=EXCLUDED.parent_id""",
        rows
    )
    con.commit()
    cur.close()
    print("  -> " + str(len(rows)))
    return len(rows)


def sync_territories(c, con):
    print("[" + c.source_id + "] dim_territories...")
    resp      = c.call("GetTerritoriesHierarhy")
    result_el = _child(resp, "GetTerritoriesHierarhyResult")
    if result_el is None:
        return 0
    rows = [
        (c.source_id, _to_uuid(_text(t, "ID")), _to_int(_text(t, "TYPE")),
         _text(t, "NAME"), _text(t, "DESC"), _to_uuid(_text(t, "PARENT_ID")))
        for t in result_el
    ]
    cur = con.cursor()
    psycopg2.extras.execute_batch(
        cur,
        """INSERT INTO dim_territories(source_id, id, type, name, descr, parent_id)
           VALUES(%s, %s, %s, %s, %s, %s)
           ON CONFLICT(source_id, id) DO UPDATE SET
               type=EXCLUDED.type, name=EXCLUDED.name,
               descr=EXCLUDED.descr, parent_id=EXCLUDED.parent_id""",
        rows
    )
    con.commit()
    cur.close()
    print("  -> " + str(len(rows)))
    return len(rows)


def sync_transaction_types(c, con):
    # Типы событий одинаковы для всех инстансов — грузим один раз
    print("[" + c.source_id + "] dim_transaction_types...")
    resp      = c.call("GetTransactionTypes", "<classMask>-1</classMask>")
    result_el = _child(resp, "GetTransactionTypesResult")
    if result_el is None:
        return 0
    rows = [
        (_to_int(_text(t, "ID")), _text(t, "NAME"), _to_int(_text(t, "CLASS_MASK")))
        for t in result_el
    ]
    cur = con.cursor()
    psycopg2.extras.execute_batch(
        cur,
        """INSERT INTO dim_transaction_types(id, name, class_mask)
           VALUES(%s, %s, %s)
           ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name, class_mask=EXCLUDED.class_mask""",
        rows
    )
    con.commit()
    cur.close()
    print("  -> " + str(len(rows)))
    return len(rows)


def sync_persons(c, con):
    print("[" + c.source_id + "] dim_persons...")
    resp      = c.call("GetOrgUnitsHierarhyWithPersons")
    result_el = _child(resp, "GetOrgUnitsHierarhyWithPersonsResult")
    if result_el is None:
        return 0
    now_utc = datetime.now(timezone.utc)
    rows    = []
    for obj in result_el:
        tab = _text(obj, "TAB_NUM")
        if tab is None:
            continue  # OrgUnit — пропускаем
        rows.append((
            c.source_id,
            _to_uuid(_text(obj, "ID")),
            _text(obj, "LAST_NAME"),
            _text(obj, "FIRST_NAME"),
            _text(obj, "MIDDLE_NAME"),
            tab,
            _to_uuid(_text(obj, "ORG_ID")),
            now_utc,
        ))
    cur = con.cursor()
    psycopg2.extras.execute_batch(
        cur,
        """INSERT INTO dim_persons
           (source_id, person_id, last_name, first_name, middle_name,
            tab_number, org_id, synced_at)
           VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT(source_id, person_id) DO UPDATE SET
               last_name=EXCLUDED.last_name,
               first_name=EXCLUDED.first_name,
               middle_name=EXCLUDED.middle_name,
               tab_number=EXCLUDED.tab_number,
               org_id=EXCLUDED.org_id,
               synced_at=EXCLUDED.synced_at""",
        rows
    )
    con.commit()
    cur.close()
    print("  -> " + str(len(rows)))
    return len(rows)


def sync_all_refs(c, con):
    sync_org_units(c, con)
    sync_territories(c, con)
    sync_transaction_types(c, con)
    sync_persons(c, con)


print("Функции справочников определены.")


# %% [markdown]
# ## 5. Выгрузка событий — один период

# %%
import time


def _dt_soap(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_territory_id(raw):
    if not raw:
        return None
    raw = raw.strip()
    for sep in (",", " ", ";"):
        if sep in raw:
            parts = [p.strip() for p in raw.split(sep) if p.strip()]
            return _to_uuid(parts[0]) if parts else None
    return _to_uuid(raw)


def _build_params_xml(date_from, date_to, transaction_types, max_result_size):
    tt_xml = ""
    for t in transaction_types:
        tt_xml += "<unsignedInt>" + str(t) + "</unsignedInt>"
    return (
        "<parameters>"
        + "<StartDate>" + _dt_soap(date_from) + "</StartDate>"
        + "<EndDate>"   + _dt_soap(date_to)   + "</EndDate>"
        + "<TransactionTypes>" + tt_xml + "</TransactionTypes>"
        + "<MaxResultSize>" + str(max_result_size) + "</MaxResultSize>"
        + "</parameters>"
    )


def _build_fields_xml(field_guids):
    inner = ""
    for g in field_guids:
        inner += "<guid>" + g + "</guid>"
    return "<fields>" + inner + "</fields>"


def fetch_period_events(c, date_from, date_to,
                         transaction_types=None,
                         field_guids=None,
                         max_result_size=MAX_RESULT_SIZE,
                         chunk_size=CHUNK_SIZE):
    """
    Возвращает list of tuples:
    (source_id, event_guid, event_dt, transaction_type_id,
     person_id, territory_id, card_code)
    """
    if transaction_types is None: transaction_types = TRANSACTION_TYPES
    if field_guids       is None: field_guids       = EVENT_FIELD_GUIDS

    params_xml = _build_params_xml(date_from, date_to, transaction_types, max_result_size)
    resp       = c.call("OpenEventHistorySession", params_xml)
    hist_id    = _guid_result(resp, "OpenEventHistorySession")

    try:
        resp     = c.call(
            "GetEventHistoryResultCount",
            "<eventHistorySessionID>" + hist_id + "</eventHistorySessionID>"
        )
        count_el = _child(resp, "GetEventHistoryResultCountResult")
        total    = int(count_el.text) if (count_el is not None and count_el.text) else 0
        print("  Событий: " + str(total))

        if total == 0:
            return []

        fields_xml = _build_fields_xml(field_guids)
        all_rows   = []
        t0         = time.time()

        for offset in range(0, total, chunk_size):
            n    = min(chunk_size, total - offset)
            resp = c.call(
                "GetEventHistoryResult",
                "<eventHistorySessionID>" + hist_id + "</eventHistorySessionID>"
                + fields_xml
                + "<offset>" + str(offset) + "</offset>"
                + "<count>"  + str(n)      + "</count>"
            )
            result_el = _child(resp, "GetEventHistoryResultResult")
            if result_el is None:
                continue

            for ev_obj in result_el:
                values_el = _child(ev_obj, "Values")
                if values_el is None:
                    continue
                vals = [ch.text for ch in values_el]
                while len(vals) < 6:
                    vals.append(None)

                event_guid = vals[0]
                if not event_guid:
                    continue

                event_dt = None
                if vals[1]:
                    try:
                        event_dt = datetime.fromisoformat(
                            vals[1].strip().rstrip("Z")
                        ).replace(tzinfo=timezone.utc)
                    except Exception:
                        pass

                all_rows.append((
                    c.source_id,
                    event_guid.strip(),
                    event_dt,
                    _to_int(vals[2]),
                    _to_uuid(vals[3]),
                    _parse_territory_id(vals[4]),
                    vals[5],
                ))

            done  = offset + n
            speed = done / (time.time() - t0) if (time.time() - t0) > 0 else 0
            print(
                "\r    " + str(round(done / total * 100, 1)) + "%"
                + " " + str(done) + "/" + str(total)
                + " (" + str(round(speed)) + " зап/с)   ",
                end="", flush=True
            )

        print("")
        return all_rows

    finally:
        try:
            c._post("CloseEventHistorySession",
                    "<sessionID>" + c.session_id + "</sessionID>"
                    + "<eventHistorySessionID>" + hist_id + "</eventHistorySessionID>")
        except Exception:
            pass


def _insert_events(con, rows):
    if not rows:
        return 0
    cur = con.cursor()
    psycopg2.extras.execute_batch(
        cur,
        """INSERT INTO events
           (source_id, event_guid, event_dt, transaction_type_id,
            person_id, territory_id, card_code)
           VALUES(%s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT(source_id, event_guid) DO NOTHING""",
        rows
    )
    inserted = cur.rowcount
    con.commit()
    cur.close()
    return inserted


print("fetch_period_events и _insert_events определены.")


# %% [markdown]
# ## 6. Историческая загрузка

# %%

def _month_periods(start, end):
    periods = []
    cur = start.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    while cur < end:
        nxt = cur.replace(month=cur.month + 1) if cur.month < 12 \
              else cur.replace(year=cur.year + 1, month=1)
        nxt = min(nxt, end)
        periods.append((cur, nxt))
        cur = nxt
    return periods


def _init_sync_log(con, source_id, start, end):
    periods = _month_periods(start, end)
    cur     = con.cursor()
    for p_start, p_end in periods:
        cur.execute(
            """INSERT INTO _sync_log(source_id, period_start, period_end, status)
               VALUES(%s, %s, %s, 'pending')
               ON CONFLICT(source_id, period_start) DO NOTHING""",
            (source_id, p_start, p_end)
        )
    con.commit()
    cur.close()
    return len(periods)


def _load_source_history(source_cfg, con, history_start, history_end, transaction_types):
    """Историческая загрузка для одного инстанса."""
    sid = source_cfg["source_id"]
    n   = _init_sync_log(con, sid, history_start, history_end)
    print("[" + sid + "] Периодов добавлено в очередь: " + str(n))

    cur = con.cursor()
    cur.execute(
        """SELECT period_start, period_end FROM _sync_log
           WHERE source_id=%s AND status != 'done'
           ORDER BY period_start""",
        (sid,)
    )
    pending = cur.fetchall()
    cur.close()

    if not pending:
        print("[" + sid + "] Все периоды уже загружены.")
        return

    print("[" + sid + "] Осталось периодов: " + str(len(pending)))

    with ParsecClient(source_cfg) as c:
        sync_all_refs(c, con)

        for p_start, p_end in pending:
            label = str(p_start)[:7]
            print("\n[" + sid + "][" + label + "] Загрузка...")

            cur = con.cursor()
            cur.execute(
                """UPDATE _sync_log SET status='running', started_at=%s
                   WHERE source_id=%s AND period_start=%s""",
                (datetime.now(timezone.utc), sid, p_start)
            )
            con.commit()
            cur.close()

            try:
                rows     = fetch_period_events(c, p_start, p_end, transaction_types)
                inserted = _insert_events(con, rows)

                cur = con.cursor()
                cur.execute(
                    """UPDATE _sync_log
                       SET status='done', rows_loaded=%s, finished_at=%s, error_msg=NULL
                       WHERE source_id=%s AND period_start=%s""",
                    (len(rows), datetime.now(timezone.utc), sid, p_start)
                )
                con.commit()
                cur.close()
                print("  Загружено: " + str(len(rows)) + " | Новых: " + str(inserted))

            except Exception as e:
                err_s = str(e)[:500]
                cur   = con.cursor()
                cur.execute(
                    """UPDATE _sync_log SET status='error', error_msg=%s
                       WHERE source_id=%s AND period_start=%s""",
                    (err_s, sid, p_start)
                )
                con.commit()
                cur.close()
                print("  ОШИБКА: " + err_s)
                continue


def load_history(pg_url=PG_URL,
                  history_start=None,
                  history_end=None,
                  transaction_types=None,
                  sources=None):
    """
    Шаг 1: историческая загрузка по всем инстансам последовательно.
    При повторном запуске продолжает с незавершённых периодов.

    sources — список конфигов из SOURCES, по умолчанию все.
    """
    if history_start     is None: history_start     = HISTORY_START
    if history_end       is None: history_end       = datetime.now(timezone.utc)
    if transaction_types is None: transaction_types = TRANSACTION_TYPES
    if sources           is None: sources           = SOURCES

    con = init_db(pg_url)

    for src in sources:
        print("\n=== Инстанс: " + src["source_id"] + " (" + src["city"] + ") ===")
        try:
            _load_source_history(src, con, history_start, history_end, transaction_types)
        except Exception as e:
            print("КРИТИЧЕСКАЯ ОШИБКА инстанса " + src["source_id"] + ": " + str(e))
            continue

    # Итог
    cur = con.cursor()
    cur.execute("SELECT source_id, COUNT(*) FROM events GROUP BY source_id ORDER BY source_id")
    counts = cur.fetchall()
    cur.execute("SELECT COUNT(*) FROM _sync_log WHERE status='error'")
    err_total = cur.fetchone()[0]
    cur.close()
    con.close()

    print("\n=== Историческая загрузка завершена ===")
    for sid, cnt in counts:
        print("  [" + sid + "] событий: " + str(cnt))
    print("  Периодов с ошибкой: " + str(err_total))


print("load_history определена.")


# %% [markdown]
# ## 7. Инкрементальная загрузка

# %%

def load_incremental(pg_url=PG_URL,
                      lookback_hours=25,
                      transaction_types=None,
                      sync_refs=True,
                      sources=None):
    """
    Шаг 2: для каждого инстанса берёт MAX(event_dt) - lookback_hours
    и тянет события до текущего момента.
    """
    if transaction_types is None: transaction_types = TRANSACTION_TYPES
    if sources           is None: sources           = SOURCES

    con     = init_db(pg_url)
    date_to = datetime.now(timezone.utc)

    for src in sources:
        sid = src["source_id"]
        print("\n=== Инкремент: " + sid + " ===")

        cur = con.cursor()
        cur.execute("SELECT MAX(event_dt) FROM events WHERE source_id=%s", (sid,))
        row = cur.fetchone()
        cur.close()

        if not row or row[0] is None:
            print("  БД пуста для " + sid + " — запусти load_history() сначала.")
            continue

        date_from = row[0] - timedelta(hours=lookback_hours)
        print("  " + str(date_from)[:16] + " UTC -> " + str(date_to)[:16] + " UTC")

        try:
            with ParsecClient(src) as c:
                if sync_refs:
                    sync_all_refs(c, con)
                rows = fetch_period_events(c, date_from, date_to, transaction_types)

            inserted = _insert_events(con, rows)
            print("  Получено: " + str(len(rows)) + " | Новых: " + str(inserted))

        except Exception as e:
            print("  ОШИБКА: " + str(e))
            continue

    cur = con.cursor()
    cur.execute("SELECT source_id, COUNT(*) FROM events GROUP BY source_id ORDER BY source_id")
    counts = cur.fetchall()
    cur.close()
    con.close()

    print("\nВсего в БД:")
    for sid, cnt in counts:
        print("  [" + sid + "] " + str(cnt) + " событий")


print("load_incremental определена.")


# %% [markdown]
# ## 8. Состояние БД

# %%

def db_status(pg_url=PG_URL):
    con = pg_connect(pg_url)
    cur = con.cursor()

    cur.execute("""
        SELECT e.source_id, s.city, COUNT(*), MIN(e.event_dt), MAX(e.event_dt)
        FROM events e
        JOIN dim_sources s ON s.source_id = e.source_id
        GROUP BY e.source_id, s.city
        ORDER BY e.source_id
    """)
    ev_rows = cur.fetchall()

    cur.execute("SELECT source_id, COUNT(*) FROM dim_persons  GROUP BY source_id ORDER BY source_id")
    pers_rows = cur.fetchall()

    cur.execute("SELECT source_id, COUNT(*) FROM dim_org_units GROUP BY source_id ORDER BY source_id")
    org_rows = cur.fetchall()

    cur.execute("SELECT source_id, COUNT(*) FROM dim_territories GROUP BY source_id ORDER BY source_id")
    terr_rows = cur.fetchall()

    cur.execute("SELECT COUNT(*) FROM dim_transaction_types")
    ttypes = cur.fetchone()[0]

    cur.execute("SELECT source_id, status, COUNT(*) FROM _sync_log GROUP BY source_id, status ORDER BY source_id, status")
    sync_rows = cur.fetchall()

    cur.execute("SELECT source_id, period_start, error_msg FROM _sync_log WHERE status='error' ORDER BY source_id, period_start")
    err_rows = cur.fetchall()

    # Матч сотрудников между инстансами по tab_number
    cur.execute("""
        SELECT COUNT(DISTINCT tab_number)
        FROM dim_persons
        WHERE tab_number IS NOT NULL AND tab_number != ''
          AND tab_number IN (
            SELECT tab_number FROM dim_persons
            WHERE source_id = %s AND tab_number IS NOT NULL AND tab_number != ''
          )
          AND tab_number IN (
            SELECT tab_number FROM dim_persons
            WHERE source_id = %s AND tab_number IS NOT NULL AND tab_number != ''
          )
    """, (SOURCES[0]["source_id"], SOURCES[1]["source_id"])) if len(SOURCES) >= 2 else None
    matched_row = cur.fetchone() if len(SOURCES) >= 2 else None

    cur.close()
    con.close()

    print("=== Состояние БД ===\n")
    print("Events:")
    for sid, city, cnt, mn, mx in ev_rows:
        print("  [" + sid + "] " + city + ": " + str(cnt) + " (" + str(mn)[:10] + " -> " + str(mx)[:10] + ")")

    print("\ndim_persons:")
    pers_d = {r[0]: r[1] for r in pers_rows}
    for src in SOURCES:
        print("  [" + src["source_id"] + "]: " + str(pers_d.get(src["source_id"], 0)))
    if matched_row:
        print("  Совпадений по tab_number между инстансами: " + str(matched_row[0]))

    print("\ndim_org_units:")
    org_d = {r[0]: r[1] for r in org_rows}
    for src in SOURCES:
        print("  [" + src["source_id"] + "]: " + str(org_d.get(src["source_id"], 0)))

    print("\ndim_territories:")
    terr_d = {r[0]: r[1] for r in terr_rows}
    for src in SOURCES:
        print("  [" + src["source_id"] + "]: " + str(terr_d.get(src["source_id"], 0)))

    print("\ndim_transaction_types: " + str(ttypes))

    print("\n_sync_log:")
    for sid, status, cnt in sync_rows:
        print("  [" + sid + "] " + str(status).ljust(10) + str(cnt))

    if err_rows:
        print("\nОшибочные периоды:")
        for sid, period, msg in err_rows:
            print("  [" + sid + "] " + str(period)[:10] + " -> " + str(msg)[:80])


print("db_status определена.")


# %% [markdown]
# ## 9. Матчинг сотрудников между инстансами
#
# Если база сотрудников общая — можно автоматически заполнить `canonical_person_id`
# по совпадению `tab_number`. Это позволит в аналитике видеть одного человека
# в двух городах как одну сущность.

# %%

def match_persons_by_tab(pg_url=PG_URL, dry_run=True):
    """
    Находит сотрудников с одинаковым tab_number в разных инстансах
    и заполняет canonical_person_id (берём person_id из первого инстанса).

    dry_run=True  — только показывает сколько совпадений, ничего не пишет.
    dry_run=False — реально обновляет canonical_person_id.
    """
    if len(SOURCES) < 2:
        print("Нужно минимум 2 инстанса.")
        return

    src_a = SOURCES[0]["source_id"]
    src_b = SOURCES[1]["source_id"]

    con = pg_connect(pg_url)
    cur = con.cursor()

    cur.execute("""
        SELECT a.tab_number, a.person_id AS pid_a, b.person_id AS pid_b,
               a.last_name, a.first_name
        FROM dim_persons a
        JOIN dim_persons b ON b.tab_number = a.tab_number
                          AND b.source_id  = %s
        WHERE a.source_id = %s
          AND a.tab_number IS NOT NULL
          AND a.tab_number != ''
        ORDER BY a.tab_number
    """, (src_b, src_a))
    matches = cur.fetchall()

    print("Совпадений по tab_number: " + str(len(matches)))
    if matches:
        print("Первые 10:")
        for tab, pid_a, pid_b, ln, fn in matches[:10]:
            match_str = "  " + tab + " | " + str(pid_a) + " | " + str(pid_b)
            if ln or fn:
                match_str += " | " + str(ln) + " " + str(fn)
            print(match_str)

    if dry_run:
        print("\ndry_run=True — ничего не записано. Передай dry_run=False для применения.")
        cur.close()
        con.close()
        return

    # Canonical = person_id из инстанса A (первый в SOURCES)
    updated = 0
    for tab, pid_a, pid_b, _, _ in matches:
        cur.execute(
            "UPDATE dim_persons SET canonical_person_id=%s WHERE source_id=%s AND person_id=%s",
            (pid_a, src_a, pid_a)
        )
        cur.execute(
            "UPDATE dim_persons SET canonical_person_id=%s WHERE source_id=%s AND person_id=%s",
            (pid_a, src_b, pid_b)
        )
        updated += 2

    con.commit()
    cur.close()
    con.close()
    print("Обновлено записей: " + str(updated))


print("match_persons_by_tab определена.")


# %% [markdown]
# ## 10. Запуск

# %%

# Обязательно указать PG_URL и SOURCES в ячейке 1.

# ── Шаг 1: историческая загрузка (один раз) ───────────────────────────────────
# load_history()

# ── Проверка состояния ────────────────────────────────────────────────────────
# db_status()

# ── Опциональный матчинг сотрудников (после load_history) ────────────────────
# match_persons_by_tab(dry_run=True)   # сначала смотрим
# match_persons_by_tab(dry_run=False)  # потом применяем

# ── Шаг 2: инкремент (ежедневно) ─────────────────────────────────────────────
# load_incremental()

print("Готово.")
