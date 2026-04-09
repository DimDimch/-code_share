# =============================================================================
# collect.py — сбор данных из СКУД Parsec в PostgreSQL
# httpx + lxml + psycopg2, без f-строк, без zeep
#
# Запуск из Jupyter: запусти все ячейки, затем ячейку "## 9. Точка входа"
#
# Зависимости:
#   pip install httpx lxml psycopg2-binary tqdm
# =============================================================================

# %% [markdown]
# ## 0. Импорты и параметры запуска

# %%
from datetime import datetime, timezone, timedelta

import httpx
from lxml import etree
import psycopg2
import psycopg2.extras
from tqdm import tqdm

from config import PARSEC_INSTANCES, SITES, PG_DSN, COLLECT


# =============================================================================
# Параметры запуска — меняй здесь перед запуском
# =============================================================================

# Площадка для сбора. Доступные ключи — см. config.py, раздел SITES.
RUN_SITE = "site_a"

# True  — синхронизировать сотрудников (parsecnew_persons)
# False — пропустить
RUN_PERSONS = True

# True  — собирать события (parsecnew_events)
# False — пропустить
RUN_EVENTS = True


# %% [markdown]
# ## 1. Константы из конфига

# %%

NS_PARSEC = "http://parsec.ru/Parsec3IntergationService"
NS_SOAP   = "http://schemas.xmlsoap.org/soap/envelope/"

# Коды типов транзакций → направление прохода
ENTRY_CODES = {590144, 590152}
EXIT_CODES  = {590145, 590153, 590146, 590244, 590245}

# GUIDы полей, которые запрашиваем из истории событий
EVENT_FIELD_GUIDS = [
    "9f7a30e6-c9ed-4e62-83e3-59032a0f8d27",  # event_guid
    "2c5ee108-28e3-4dcc-8c95-7f3222d8e67f",  # event_dt
    "57ca38e4-ed6f-4d12-adcb-2faa16f950d7",  # transaction_type_id
    "7c6d82a0-c8c8-495b-9728-357807193d23",  # person_id
    "42dab9c6-5d30-4030-8ccd-2cad6fcbc5f2",  # territory_ids (массив)
    "0de358e0-c91b-4333-b902-000000000005",  # card_code
]


# %% [markdown]
# ## 2. Вспомогательные функции парсинга XML

# %%

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


def _children(node, local_name):
    return [ch for ch in node if etree.QName(ch.tag).localname == local_name]


def _to_uuid(val):
    if not val:
        return None
    val = val.strip()
    empty = "00000000-0000-0000-0000-000000000000"
    if val == empty:
        return None
    return val if (len(val) == 36 and val.count("-") == 4) else None


def _to_int(val):
    try:    return int(val)
    except: return None


def _parse_dt(val):
    """ISO 8601 строка → datetime UTC. Parsec отдаёт UTC."""
    if not val:
        return None
    val = val.strip()
    # Убираем дробные секунды для единообразия
    if "." in val:
        val = val[:val.index(".")] + "Z" if val.endswith("Z") else val[:val.index(".")]
    # Добавляем tzinfo если нет
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(val, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


# %% [markdown]
# ## 3. SOAP-клиент

# %%

class ParsecClient:

    def __init__(self, instance_cfg, source_id):
        self.source_id = source_id
        self.config    = instance_cfg
        self.base_url  = (
            "http://" + instance_cfg["host_addr"]
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
        print("[" + self.source_id + "] Сессия открыта: " + str(self.session_id))
        return self

    def disconnect(self):
        if self.session_id and self._http:
            try:
                self._post("CloseSession",
                           "<sessionID>" + self.session_id + "</sessionID>")
                print("[" + self.source_id + "] Сессия закрыта.")
            except Exception as e:
                print("[" + self.source_id + "] Предупреждение при закрытии: " + str(e))
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


# %% [markdown]
# ## 4. Подключение к PostgreSQL

# %%

def pg_connect():
    con = psycopg2.connect(PG_DSN)
    con.autocommit = False
    return con


# %% [markdown]
# ## 5. Построение карты зон для площадки

# %%

def build_zone_map(site_cfg):
    """
    Возвращает:
      zone_map  — dict territory_name -> zone_type ('territory'/'building'/'production')
      ap_map    — dict territory_name -> access_point_type ('turnstile'/'door')
      all_names — set всех territory_name площадки (для фильтрации событий)
    """
    zone_map  = {}
    ap_map    = site_cfg.get("access_point_types", {})
    all_names = set()

    for zone_type, names in site_cfg["zones"].items():
        for name in names:
            zone_map[name]  = zone_type
            all_names.add(name)

    return zone_map, ap_map, all_names


# %% [markdown]
# ## 6. Синхронизация сотрудников

# %%

def sync_persons(client, con, site_id, site_cfg):
    """
    Загружает полную иерархию подразделений с сотрудниками из СКУД.
    Сохраняет в parsecnew_persons (upsert).
    """
    print("\n[" + site_id + "] Синхронизация сотрудников...")

    resp      = client.call("GetOrgUnitsHierarhyWithPersons")
    result_el = _child(resp, "GetOrgUnitsHierarhyWithPersonsResult")
    if result_el is None:
        print("  Пустой ответ.")
        return 0

    # Сначала собираем справочник подразделений id -> name
    org_map = {}
    for obj in result_el:
        tab = _text(obj, "TAB_NUM")
        if tab is None:
            # Это OrgUnit, не Person
            uid  = _to_uuid(_text(obj, "ID"))
            name = _text(obj, "NAME")
            if uid:
                org_map[uid] = name

    # Второй проход — сотрудники
    resp      = client.call("GetOrgUnitsHierarhyWithPersons")
    result_el = _child(resp, "GetOrgUnitsHierarhyWithPersonsResult")

    rows    = []
    now_utc = datetime.now(timezone.utc)

    for obj in result_el:
        tab = _text(obj, "TAB_NUM")
        if tab is None:
            continue  # OrgUnit
        person_id = _to_uuid(_text(obj, "ID"))
        if not person_id:
            continue
        org_id   = _to_uuid(_text(obj, "ORG_ID"))
        dept_name = org_map.get(org_id, None) if org_id else None

        rows.append((
            site_id,
            person_id,
            _text(obj, "LAST_NAME"),
            _text(obj, "FIRST_NAME"),
            _text(obj, "MIDDLE_NAME"),
            tab,
            dept_name,
            now_utc,
        ))

    if not rows:
        print("  Сотрудников не найдено.")
        return 0

    cur = con.cursor()
    psycopg2.extras.execute_batch(
        cur,
        """
        INSERT INTO parsecnew_persons
            (site_id, person_id, last_name, first_name, middle_name,
             tab_number, department_name, synced_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (site_id, person_id) DO UPDATE SET
            last_name       = EXCLUDED.last_name,
            first_name      = EXCLUDED.first_name,
            middle_name     = EXCLUDED.middle_name,
            tab_number      = EXCLUDED.tab_number,
            department_name = EXCLUDED.department_name,
            synced_at       = EXCLUDED.synced_at
        """,
        rows,
        page_size=500,
    )
    con.commit()
    cur.close()
    print("  Сотрудников в БД: " + str(len(rows)))
    return len(rows)


# %% [markdown]
# ## 7. Сбор событий

# %%

def _build_fields_xml():
    """XML-блок с перечислением GUIDов запрашиваемых полей."""
    parts = []
    for guid in EVENT_FIELD_GUIDS:
        parts.append("<guid>" + guid + "</guid>")
    return "<fields>" + "".join(parts) + "</fields>"


def _build_transaction_types_xml():
    parts = []
    for code in COLLECT["transaction_types"]:
        parts.append("<unsignedInt>" + str(code) + "</unsignedInt>")
    return "<TransactionTypes>" + "".join(parts) + "</TransactionTypes>"


def _open_history_session(client, date_from, date_to):
    """
    Открывает сессию истории событий с нужными фильтрами.
    Возвращает GUID сессии истории.
    """
    date_from_str = date_from.strftime("%Y-%m-%dT%H:%M:%SZ")
    date_to_str   = date_to.strftime("%Y-%m-%dT%H:%M:%SZ")

    params_xml = (
        "<parameters>"
        + "<StartDate>" + date_from_str + "</StartDate>"
        + "<EndDate>"   + date_to_str   + "</EndDate>"
        + _build_transaction_types_xml()
        + "<MaxResultSize>" + str(COLLECT["max_result_size"]) + "</MaxResultSize>"
        + "</parameters>"
    )

    resp      = client.call("OpenEventHistorySession", params_xml)
    result_el = _child(resp, "OpenEventHistorySessionResult")
    if result_el is None:
        raise RuntimeError("OpenEventHistorySession: пустой ответ")
    if _text(result_el, "Result") == "-1":
        raise RuntimeError(
            "OpenEventHistorySession: " + (_text(result_el, "ErrorMessage") or "")
        )
    session_guid = _text(result_el, "Value")
    if not session_guid:
        raise RuntimeError("OpenEventHistorySession: не получен GUID сессии")
    return session_guid


def _get_history_count(client, history_session_id):
    resp = client.call(
        "GetEventHistoryResultCount",
        "<eventHistorySessionID>" + history_session_id + "</eventHistorySessionID>",
    )
    result_el = _child(resp, "GetEventHistoryResultCountResult")
    try:
        return int(result_el.text or "0")
    except (TypeError, ValueError):
        return 0


def _get_history_chunk(client, history_session_id, offset, count):
    """
    Запрашивает чанк событий. Возвращает список элементов EventObject.
    """
    extra = (
        "<eventHistorySessionID>" + history_session_id + "</eventHistorySessionID>"
        + _build_fields_xml()
        + "<offset>" + str(offset) + "</offset>"
        + "<count>"  + str(count)  + "</count>"
    )
    resp      = client.call("GetEventHistoryResult", extra)
    result_el = _child(resp, "GetEventHistoryResultResult")
    if result_el is None:
        return []
    return list(result_el)


def _parse_event_object(obj_el):
    """
    Разбирает один EventObject (массив Values) → dict с нужными полями.
    Поля соответствуют порядку EVENT_FIELD_GUIDS.
    """
    values = _children(obj_el, "Values") if obj_el is not None else []

    def _val(idx):
        if idx < len(values):
            v = values[idx]
            # Значение может быть прямым текстом элемента или в дочернем string/...
            text = (v.text or "").strip()
            if text:
                return text
            # Массив GUID (territory_ids)
            guids = _children(v, "guid")
            if guids:
                return [((g.text or "").strip()) for g in guids]
        return None

    raw_guid   = _val(0)
    raw_dt     = _val(1)
    raw_type   = _val(2)
    raw_person = _val(3)
    raw_terr   = _val(4)   # может быть список
    raw_card   = _val(5)

    # territory_id — берём первый из массива (обычно он один)
    if isinstance(raw_terr, list):
        territory_id = _to_uuid(raw_terr[0]) if raw_terr else None
    else:
        territory_id = _to_uuid(raw_terr)

    return {
        "event_guid":          raw_guid,
        "event_dt":            _parse_dt(raw_dt),
        "transaction_type_id": _to_int(raw_type),
        "person_id":           _to_uuid(raw_person),
        "territory_id":        territory_id,
        "card_code":           raw_card,
    }


def _close_history_session(client, history_session_id):
    try:
        client.call(
            "CloseEventHistorySession",
            "<eventHistorySessionID>" + history_session_id + "</eventHistorySessionID>",
        )
    except Exception as e:
        print("  Предупреждение: CloseEventHistorySession: " + str(e))


def fetch_and_store_events(client, con, site_id, site_cfg, date_from, date_to):
    """
    Основная функция сбора событий:
    1. Открывает сессию истории
    2. Получает общее число событий
    3. Загружает чанками с прогрессом
    4. Фильтрует по зонам площадки
    5. Вставляет в parsecnew_events
    """
    zone_map, ap_map, all_names = build_zone_map(site_cfg)
    chunk_size = COLLECT["chunk_size"]
    tz_offset  = timedelta(hours=COLLECT["local_tz_offset_hours"])

    print("\n[" + site_id + "] Открываю сессию истории событий...")
    history_session_id = _open_history_session(client, date_from, date_to)

    total = _get_history_count(client, history_session_id)
    print("[" + site_id + "] Событий в СКУД за период: " + str(total))

    if total == 0:
        _close_history_session(client, history_session_id)
        print("[" + site_id + "] Событий нет. Завершено.")
        return 0

    # Получаем справочник территорий из БД (если уже был другой сбор)
    # и из СКУД напрямую через GetTerritoriesHierarhy
    terr_name_map = _load_territory_names(client)

    inserted_total = 0
    skipped_total  = 0

    rows_batch = []

    with tqdm(
        total=total,
        desc="[" + site_id + "] события",
        unit="событий",
        ncols=80,
    ) as pbar:

        for offset in range(0, total, chunk_size):
            count = min(chunk_size, total - offset)
            chunk = _get_history_chunk(client, history_session_id, offset, count)

            for obj_el in chunk:
                ev = _parse_event_object(obj_el)

                # Пропускаем если нет времени или человека
                if not ev["event_dt"] or not ev["event_guid"]:
                    skipped_total += 1
                    continue

                # Определяем direction
                t_id = ev["transaction_type_id"]
                if t_id in ENTRY_CODES:
                    direction = "entry"
                elif t_id in EXIT_CODES:
                    direction = "exit"
                else:
                    skipped_total += 1
                    continue

                # Разрешаем название территории
                terr_id   = ev["territory_id"]
                terr_name = terr_name_map.get(terr_id) if terr_id else None

                # Фильтруем: только точки прохода нашей площадки
                if terr_name not in all_names:
                    skipped_total += 1
                    continue

                zone_type  = zone_map.get(terr_name)
                ap_type    = ap_map.get(terr_name, "door")

                event_dt     = ev["event_dt"]
                event_dt_msk = event_dt + tz_offset

                rows_batch.append((
                    site_id,
                    ev["event_guid"],
                    event_dt,
                    event_dt_msk,
                    t_id,
                    direction,
                    ev["person_id"],
                    terr_id,
                    terr_name,
                    zone_type,
                    ap_type,
                    ev["card_code"],
                ))

            # Сбрасываем батч каждые ~5000 строк или в конце
            if len(rows_batch) >= 5000 or (offset + count >= total):
                n = _insert_events_batch(con, rows_batch)
                inserted_total += n
                rows_batch = []

            pbar.update(len(chunk))

    _close_history_session(client, history_session_id)

    print(
        "[" + site_id + "] Готово."
        + " Вставлено: " + str(inserted_total)
        + " | Пропущено (не наша зона / нет данных): " + str(skipped_total)
    )
    return inserted_total


def _load_territory_names(client):
    """
    Загружает словарь territory_id (UUID) -> territory_name из СКУД.
    Нужен для обогащения событий именем территории при вставке.
    """
    terr_map = {}
    resp      = client.call("GetTerritoriesHierarhy")
    result_el = _child(resp, "GetTerritoriesHierarhyResult")
    if result_el is None:
        return terr_map
    for t in result_el:
        uid  = _to_uuid(_text(t, "ID"))
        name = _text(t, "NAME")
        if uid and name:
            terr_map[uid] = name
    return terr_map


def _insert_events_batch(con, rows):
    if not rows:
        return 0
    cur = con.cursor()
    psycopg2.extras.execute_batch(
        cur,
        """
        INSERT INTO parsecnew_events
            (site_id, event_guid, event_dt, event_dt_msk,
             transaction_type_id, direction,
             person_id, territory_id, territory_name,
             zone_type, access_point_type, card_code)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (site_id, event_guid) DO NOTHING
        """,
        rows,
        page_size=500,
    )
    inserted = cur.rowcount
    con.commit()
    cur.close()
    return inserted if inserted >= 0 else len(rows)


# %% [markdown]
# ## 8. Сводка после сбора

# %%

def print_summary(con, site_id):
    cur = con.cursor()

    cur.execute(
        """
        SELECT
            zone_type,
            direction,
            COUNT(*) AS cnt,
            MIN(event_dt_msk) AS dt_min,
            MAX(event_dt_msk) AS dt_max
        FROM parsecnew_events
        WHERE site_id = %s
        GROUP BY zone_type, direction
        ORDER BY zone_type, direction
        """,
        (site_id,),
    )
    rows = cur.fetchall()

    cur.execute(
        "SELECT COUNT(*) FROM parsecnew_persons WHERE site_id = %s",
        (site_id,),
    )
    persons_count = cur.fetchone()[0]

    cur.execute(
        "SELECT COUNT(DISTINCT person_id) FROM parsecnew_events WHERE site_id = %s",
        (site_id,),
    )
    persons_with_events = cur.fetchone()[0]

    cur.close()

    print("\n=== Итог по площадке [" + site_id + "] ===")
    print("Сотрудников в справочнике: " + str(persons_count))
    print("Сотрудников с событиями:   " + str(persons_with_events))
    print("")
    print("  Зона            Направление   Событий  Первое              Последнее")
    print("  " + "-" * 75)
    for zone, direction, cnt, dt_min, dt_max in rows:
        zone_str   = (zone or "NULL").ljust(15)
        dir_str    = (direction or "").ljust(13)
        cnt_str    = str(cnt).rjust(8)
        min_str    = str(dt_min)[:16] if dt_min else "—"
        max_str    = str(dt_max)[:16] if dt_max else "—"
        print("  " + zone_str + " " + dir_str + " " + cnt_str + "  " + min_str + "  " + max_str)


# %% [markdown]
# ## 9. Точка входа

# %%

def main():
    site_id  = RUN_SITE
    site_cfg = SITES[site_id]

    instance_key = site_cfg["parsec_instance"]
    instance_cfg = PARSEC_INSTANCES[instance_key]

    print("=== Сбор данных СКУД ===")
    print("Площадка:  " + site_cfg["display_name"])
    print("Инстанс:   " + instance_key + " (" + instance_cfg["host_addr"] + ")")
    print(
        "Период:    "
        + str(COLLECT["date_from"].date())
        + " — "
        + str(COLLECT["date_to"].date())
        + " UTC"
    )

    con = pg_connect()

    try:
        with ParsecClient(instance_cfg, site_id) as client:

            if RUN_PERSONS:
                sync_persons(client, con, site_id, site_cfg)

            if RUN_EVENTS:
                fetch_and_store_events(
                    client,
                    con,
                    site_id,
                    site_cfg,
                    COLLECT["date_from"],
                    COLLECT["date_to"],
                )

        print_summary(con, site_id)

    finally:
        con.close()


main()
