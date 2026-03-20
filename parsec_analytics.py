# %% [markdown]
# # Parsec ACS — выгрузка данных пропускного режима
# Реализация без zeep: чистый httpx + lxml

# %% [markdown]
# ## 0. Зависимости

# %%
# pip install httpx lxml pandas openpyxl
# import subprocess
# subprocess.run(["pip", "install", "httpx", "lxml", "pandas", "openpyxl"])


# %% [markdown]
# ## 1. Конфигурация

# %%
from datetime import datetime, timezone, timedelta

CONNECTION = {
    "host_addr":   "127.0.0.1",
    "organization": "SYSTEM",
    "username":    "parsec",
    "password":    "parsec",
    # таймаут httpx в секундах
    "timeout":     30,
}

DATE_FROM = datetime(2024,  1,  1, tzinfo=timezone.utc)
DATE_TO   = datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

TRANSACTION_TYPES = [590144, 590145, 590152, 590153, 590146, 590244, 590245]

MAX_RESULT_SIZE   = 10_000_000
CHUNK_SIZE        = 500
LOCAL_TZ_OFFSET_HOURS = 3   # UTC+3

# GUID → имя столбца (порядок важен — Values возвращаются в этом порядке)
EVENT_FIELDS = {
    "2c5ee108-28e3-4dcc-8c95-7f3222d8e67f": "event_datetime",
    "57ca38e4-ed6f-4d12-adcb-2faa16f950d7": "transaction_code",
    "d1847aff-11aa-4ef2-aaaa-795ceefe5f9f": "transaction_name",
    "633904b5-971b-4751-96a0-92dc03d5f616": "territory_name",
    "4c5807cb-2c06-4725-9243-747e40c41d6c": "zone_name",
    "7c6d82a0-c8c8-495b-9728-357807193d23": "person_id",
    "1bf8a893-7d21-4c0c-9a2d-2e333a2d769d": "person_fullname",
    "0de358e0-c91b-4333-b902-000000000003": "person_lastname",
    "0de358e0-c91b-4333-b902-000000000001": "person_firstname",
    "0de358e0-c91b-4333-b902-000000000004": "department",
    "0de358e0-c91b-4333-b902-000000000006": "tab_number",
    "0de358e0-c91b-4333-b902-000000000005": "card_code",
}

FIELD_GUIDS   = list(EVENT_FIELDS.keys())
FIELD_COLUMNS = list(EVENT_FIELDS.values())

print("Конфигурация загружена.")


# %% [markdown]
# ## 2. SOAP-клиент

# %%
import httpx
from lxml import etree
from typing import Optional
import textwrap


# ── Namespaces ────────────────────────────────────────────────────────────────
# Обрати внимание на опечатку "Intergation" в оригинальном API Parsec —
# она в WSDL и её нужно воспроизводить точно.
NS = {
    "soap": "http://schemas.xmlsoap.org/soap/envelope/",
    "xsi":  "http://www.w3.org/2001/XMLSchema-instance",
    "xsd":  "http://www.w3.org/2001/XMLSchema",
    "p":    "http://parsec.ru/Parsec3IntergationService",
}

SOAP_ACTION_BASE = "http://parsec.ru/Parsec3IntergationService/"


def _envelope(method: str, body_inner_xml: str) -> bytes:
    """
    Оборачивает тело запроса в стандартный SOAP 1.1 конверт.
    body_inner_xml — уже готовый XML внутри <soap:Body>.
    """
    xml = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope
    xmlns:soap="{NS['soap']}"
    xmlns:xsi="{NS['xsi']}"
    xmlns:xsd="{NS['xsd']}"
    xmlns:p="{NS['p']}">
  <soap:Body>
    <p:{method}>
{textwrap.indent(body_inner_xml.strip(), "      ")}
    </p:{method}>
  </soap:Body>
</soap:Envelope>"""
    return xml.encode("utf-8")


def _parse(response_bytes: bytes) -> etree._Element:
    """Парсит SOAP-ответ, возвращает корневой элемент Body."""
    root = etree.fromstring(response_bytes)
    body = root.find(f"{{{NS['soap']}}}Body")
    if body is None:
        raise ValueError("SOAP Body не найден в ответе")
    # Если есть Fault — кидаем исключение с текстом
    fault = body.find(f"{{{NS['soap']}}}Fault")
    if fault is not None:
        fcode = _text(fault, "faultcode")
        fstr  = _text(fault, "faultstring")
        raise RuntimeError(f"SOAP Fault [{fcode}]: {fstr}")
    return body


def _text(node: etree._Element, tag: str, default=None) -> Optional[str]:
    """Первый дочерний элемент с локальным именем tag → .text."""
    for child in node:
        if etree.QName(child.tag).localname == tag:
            return (child.text or "").strip() or default
    return default


def _child(node: etree._Element, *local_names) -> Optional[etree._Element]:
    """Последовательный поиск вглубь по локальным именам тегов."""
    cur = node
    for name in local_names:
        found = None
        for ch in cur:
            if etree.QName(ch.tag).localname == name:
                found = ch
                break
        if found is None:
            return None
        cur = found
    return cur


def _children(node: etree._Element, local_name: str):
    """Все прямые дочерние элементы с данным локальным именем."""
    return [ch for ch in node if etree.QName(ch.tag).localname == local_name]


class ParsecClient:
    """
    Низкоуровневый SOAP-клиент для Parsec Integration Service.
    Работает поверх httpx, без zeep.

    Использование:
        with ParsecClient(CONNECTION) as c:
            result = c.call("GetOrgUnitsHierarhy")
    """

    def __init__(self, config: dict):
        self.config  = config
        self.base_url = (
            f"http://{config['host_addr']}:10101"
            "/IntegrationService/IntegrationService.asmx"
        )
        self._http: Optional[httpx.Client] = None
        self.session_id: Optional[str] = None

    # ── HTTP ──────────────────────────────────────────────────────────────────

    def _post(self, method: str, body_xml: str) -> etree._Element:
        """Отправляет SOAP-запрос, возвращает распарсенный Body."""
        payload = _envelope(method, body_xml)
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction":   f'"{SOAP_ACTION_BASE}{method}"',
        }
        resp = self._http.post(self.base_url, content=payload, headers=headers)
        resp.raise_for_status()
        return _parse(resp.content)

    # ── Сессия ────────────────────────────────────────────────────────────────

    def connect(self) -> "ParsecClient":
        self._http = httpx.Client(
            timeout=self.config.get("timeout", 30),
            # Parsec может отдавать невалидный SSL — отключаем проверку
            # verify=False
        )
        body = f"""
<p:domain>{self.config['organization']}</p:domain>
<p:userName>{self.config['username']}</p:userName>
<p:password>{self.config['password']}</p:password>
"""
        body_el = self._post("OpenSession", body)
        result  = _child(body_el, "OpenSessionResponse", "OpenSessionResult")
        if result is None:
            raise RuntimeError("OpenSession: неожиданная структура ответа")

        code = _text(result, "Result")
        if code == "-1":
            raise ConnectionError(f"Авторизация: {_text(result, 'ErrorMessage')}")

        session_node = _child(result, "Value")
        self.session_id = _text(session_node, "SessionID")
        print(f"Сессия открыта. SessionID: {self.session_id}")
        return self

    def disconnect(self):
        if self.session_id and self._http:
            try:
                self._post("CloseSession",
                           f"<p:sessionID>{self.session_id}</p:sessionID>")
                print("Сессия закрыта.")
            except Exception as e:
                print(f"Предупреждение при закрытии сессии: {e}")
        if self._http:
            self._http.close()
        self.session_id = None
        self._http = None

    def __enter__(self):
        return self.connect()

    def __exit__(self, *_):
        self.disconnect()

    # ── Универсальный вызов ───────────────────────────────────────────────────

    def call(self, method: str, extra_xml: str = "") -> etree._Element:
        """
        Вызывает метод с sessionID + произвольным доп. телом.
        Возвращает <{method}Response> элемент из Body.
        """
        body = f"<p:sessionID>{self.session_id}</p:sessionID>\n{extra_xml}"
        body_el  = self._post(method, body)
        response = _child(body_el, f"{method}Response")
        return response


print("ParsecClient определён.")


# %% [markdown]
# ## 3. Хелперы для разбора ответов

# %%

def _result_check(response_el: etree._Element, method: str) -> etree._Element:
    """Проверяет Result, кидает исключение при -1, возвращает <{method}Result>."""
    result_el = _child(response_el, f"{method}Result")
    if result_el is None:
        raise RuntimeError(f"{method}: элемент {method}Result не найден")
    code = _text(result_el, "Result")
    if code == "-1":
        raise RuntimeError(f"{method} error: {_text(result_el, 'ErrorMessage')}")
    return result_el


def _guid_result(response_el: etree._Element, method: str) -> str:
    """Извлекает Guid из GuidResult."""
    result_el = _result_check(response_el, method)
    return _text(result_el, "Value")


def parse_org_units(response_el: etree._Element) -> list[dict]:
    result_el = _child(response_el, "GetOrgUnitsHierarhyResult")
    if result_el is None:
        return []
    rows = []
    for unit in result_el:
        rows.append({
            "id":        _text(unit, "ID"),
            "name":      _text(unit, "NAME"),
            "desc":      _text(unit, "DESC"),
            "parent_id": _text(unit, "PARENT_ID"),
        })
    return rows


def parse_territories(response_el: etree._Element) -> list[dict]:
    result_el = _child(response_el, "GetTerritoriesHierarhyResult")
    if result_el is None:
        return []
    rows = []
    for t in result_el:
        rows.append({
            "id":        _text(t, "ID"),
            "type":      _text(t, "TYPE"),
            "name":      _text(t, "NAME"),
            "desc":      _text(t, "DESC"),
            "parent_id": _text(t, "PARENT_ID"),
        })
    return rows


def parse_transaction_types(response_el: etree._Element) -> list[dict]:
    result_el = _child(response_el, "GetTransactionTypesResult")
    if result_el is None:
        return []
    rows = []
    for t in result_el:
        rows.append({
            "id":         _text(t, "ID"),
            "name":       _text(t, "NAME"),
            "class_mask": _text(t, "CLASS_MASK"),
        })
    return rows


def parse_event_objects(response_el: etree._Element,
                         columns: list[str]) -> list[list]:
    """
    Разбирает массив EventObject[].
    Values → anyType[] → список строк в порядке запрошенных полей.
    """
    result_el = _child(response_el, "GetEventHistoryResultResult")
    if result_el is None:
        return []
    rows = []
    for ev_obj in result_el:
        values_el = _child(ev_obj, "Values")
        if values_el is None:
            rows.append([None] * len(columns))
            continue
        vals = [ch.text for ch in values_el]
        # Дополняем None если полей меньше
        while len(vals) < len(columns):
            vals.append(None)
        rows.append(vals[:len(columns)])
    return rows


print("Хелперы для парсинга определены.")


# %% [markdown]
# ## 4. Загрузка справочников

# %%
import pandas as pd


def load_org_units(c: ParsecClient) -> pd.DataFrame:
    print("Загрузка подразделений...")
    resp = c.call("GetOrgUnitsHierarhy")
    rows = parse_org_units(resp)
    df = pd.DataFrame(rows)
    print(f"  → {len(df)} подразделений")
    return df


def load_territories(c: ParsecClient) -> pd.DataFrame:
    print("Загрузка территорий...")
    resp = c.call("GetTerritoriesHierarhy")
    rows = parse_territories(resp)
    df = pd.DataFrame(rows)
    print(f"  → {len(df)} территорий")
    return df


def load_transaction_types(c: ParsecClient) -> pd.DataFrame:
    print("Загрузка типов транзакций...")
    # classMask=-1 → все категории
    extra = "<p:classMask>-1</p:classMask>"
    resp  = c.call("GetTransactionTypes", extra)
    rows  = parse_transaction_types(resp)
    df = pd.DataFrame(rows)
    print(f"  → {len(df)} типов")
    return df


# ── Раскомментируй для запуска ────────────────────────────────────────────────
# with ParsecClient(CONNECTION) as c:
#     df_orgs   = load_org_units(c)
#     df_terrs  = load_territories(c)
#     df_ttypes = load_transaction_types(c)

print("Функции справочников определены.")


# %% [markdown]
# ## 5. Выгрузка исторических событий

# %%
import time


def _dt_soap(dt: datetime) -> str:
    """datetime → строка SOAP dateTime в UTC ('yyyy-mm-ddThh:mm:ssZ')."""
    utc = dt.astimezone(timezone.utc)
    return utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_event_history_params_xml(date_from: datetime,
                                     date_to: datetime,
                                     transaction_types: list[int],
                                     max_result_size: int,
                                     territory_ids: Optional[list] = None,
                                     org_ids: Optional[list] = None,
                                     person_ids: Optional[list] = None) -> str:
    """
    Формирует XML-блок <p:parameters> для OpenEventHistorySession.
    Все datetime передаём в UTC — требование API.
    """
    tt_xml = "\n".join(
        f"<unsignedInt>{t}</unsignedInt>" for t in transaction_types
    )
    terr_xml = ""
    if territory_ids:
        terr_xml = "<Territories>" + "".join(
            f"<guid>{g}</guid>" for g in territory_ids
        ) + "</Territories>"

    org_xml = ""
    if org_ids:
        org_xml = "<Organizations>" + "".join(
            f"<guid>{g}</guid>" for g in org_ids
        ) + "</Organizations>"

    person_xml = ""
    if person_ids:
        person_xml = "<Users>" + "".join(
            f"<guid>{g}</guid>" for g in person_ids
        ) + "</Users>"

    return f"""
<p:parameters>
  <StartDate>{_dt_soap(date_from)}</StartDate>
  <EndDate>{_dt_soap(date_to)}</EndDate>
  <TransactionTypes>
    {tt_xml}
  </TransactionTypes>
  <MaxResultSize>{max_result_size}</MaxResultSize>
  {terr_xml}
  {org_xml}
  {person_xml}
</p:parameters>
""".strip()


def _build_fields_xml(field_guids: list[str]) -> str:
    """Формирует XML-блок <p:fields> для GetEventHistoryResult."""
    items = "\n".join(f"<guid>{g}</guid>" for g in field_guids)
    return f"<p:fields>\n{items}\n</p:fields>"


def fetch_event_history(c: ParsecClient,
                         date_from: datetime,
                         date_to: datetime,
                         transaction_types: list  = TRANSACTION_TYPES,
                         field_guids: list         = FIELD_GUIDS,
                         field_columns: list       = FIELD_COLUMNS,
                         max_result_size: int      = MAX_RESULT_SIZE,
                         chunk_size: int           = CHUNK_SIZE,
                         territory_ids: Optional[list] = None,
                         org_ids: Optional[list]       = None,
                         person_ids: Optional[list]    = None) -> pd.DataFrame:
    """
    Выгружает историю событий постранично, возвращает DataFrame.
    """

    # 1. Открываем сессию истории
    print("Открытие сессии истории событий...")
    params_xml = _build_event_history_params_xml(
        date_from, date_to, transaction_types, max_result_size,
        territory_ids, org_ids, person_ids
    )
    resp = c.call("OpenEventHistorySession", params_xml)
    hist_id = _guid_result(resp, "OpenEventHistorySession")
    print(f"  eventHistorySessionID: {hist_id}")

    try:
        # 2. Общее количество событий
        extra = f"<p:eventHistorySessionID>{hist_id}</p:eventHistorySessionID>"
        resp  = c.call("GetEventHistoryResultCount", extra)
        count_el = _child(resp, "GetEventHistoryResultCountResult")
        total = int(count_el.text) if count_el is not None and count_el.text else 0
        print(f"  Всего событий: {total:,}")

        if total == 0:
            return pd.DataFrame(columns=field_columns)

        # 3. Постраничная выгрузка
        fields_xml = _build_fields_xml(field_guids)
        all_rows   = []
        t0 = time.time()

        for offset in range(0, total, chunk_size):
            n = min(chunk_size, total - offset)
            extra = f"""
<p:eventHistorySessionID>{hist_id}</p:eventHistorySessionID>
{fields_xml}
<p:offset>{offset}</p:offset>
<p:count>{n}</p:count>
""".strip()
            resp = c.call("GetEventHistoryResult", extra)
            rows = parse_event_objects(resp, field_columns)
            all_rows.extend(rows)

            # Прогресс
            done    = offset + n
            elapsed = time.time() - t0
            speed   = done / elapsed if elapsed > 0 else 0
            eta     = (total - done) / speed if speed > 0 else 0
            print(
                f"\r  {done/total*100:5.1f}%  {done:{len(str(total))}}/{total}"
                f"  {speed:.0f} зап/с  ETA {eta:.0f}с   ",
                end="", flush=True
            )

        print(f"\n  Готово за {time.time()-t0:.1f}с")

    finally:
        # 4. Закрываем сессию истории — ВСЕГДА, даже при ошибке
        try:
            extra = f"<p:eventHistorySessionID>{hist_id}</p:eventHistorySessionID>"
            c._post("CloseEventHistorySession",
                    f"<p:sessionID>{c.session_id}</p:sessionID>\n{extra}")
            print("  Сессия истории закрыта.")
        except Exception as e:
            print(f"  Предупреждение: CloseEventHistorySession: {e}")

    df = pd.DataFrame(all_rows, columns=field_columns)
    print(f"  DataFrame: {df.shape[0]:,} строк × {df.shape[1]} столбцов")
    return df


print("fetch_event_history определена.")


# %% [markdown]
# ## 6. Постобработка

# %%

def postprocess_events(df: pd.DataFrame,
                        tz_offset_h: int = LOCAL_TZ_OFFSET_HOURS) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    local_tz = timezone(timedelta(hours=tz_offset_h))

    # ── datetime ──────────────────────────────────────────────────────────────
    def parse_dt(val):
        if not val:
            return pd.NaT
        try:
            dt = pd.to_datetime(val, utc=True)
            return dt
        except Exception:
            return pd.NaT

    df["event_datetime"] = df["event_datetime"].apply(parse_dt)
    df["event_datetime_local"] = df["event_datetime"].apply(
        lambda x: x.astimezone(local_tz) if pd.notna(x) else pd.NaT
    )
    df["date"]        = df["event_datetime_local"].apply(lambda x: x.date()    if pd.notna(x) else None)
    df["hour"]        = df["event_datetime_local"].apply(lambda x: x.hour      if pd.notna(x) else None)
    df["weekday_num"] = df["event_datetime_local"].apply(lambda x: x.weekday() if pd.notna(x) else None)
    DAYS = ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"]
    df["weekday"]     = df["weekday_num"].apply(lambda x: DAYS[x] if pd.notna(x) and 0<=x<=6 else None)

    # ── тип прохода ───────────────────────────────────────────────────────────
    ENTRY_CODES = {590144, 590152}
    EXIT_CODES  = {590145, 590153, 590146, 590244, 590245}

    def to_int(v):
        try: return int(v)
        except: return None

    df["transaction_code"] = df["transaction_code"].apply(to_int)
    df["is_entry"] = df["transaction_code"].apply(
        lambda c: True  if c in ENTRY_CODES else
                  False if c in EXIT_CODES  else None
    )

    # ── строки ────────────────────────────────────────────────────────────────
    str_cols = ["person_fullname","person_lastname","person_firstname",
                "department","territory_name","zone_name",
                "tab_number","card_code","transaction_name"]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace({"None":None, "nan":None})

    # ── порядок столбцов ──────────────────────────────────────────────────────
    priority = ["event_datetime_local","date","hour","weekday",
                "is_entry","transaction_code","transaction_name",
                "person_fullname","department","tab_number",
                "territory_name","zone_name","card_code","person_id",
                "event_datetime"]
    rest = [c for c in df.columns if c not in priority]
    df = df[[c for c in priority if c in df.columns] + rest]

    return df


print("postprocess_events определена.")


# %% [markdown]
# ## 7. Основной запуск

# %%
import os


def run_full_export(save_csv=True, save_excel=False, prefix="parsec_events"):
    with ParsecClient(CONNECTION) as c:
        df_orgs  = load_org_units(c)
        df_terrs = load_territories(c)
        df_raw   = fetch_event_history(
            c,
            date_from=DATE_FROM,
            date_to=DATE_TO,
        )

    df = postprocess_events(df_raw)
    print(f"\nИтог: {len(df):,} событий")

    if save_csv:
        df.to_csv(f"{prefix}.csv",     index=False, encoding="utf-8-sig")
        df_orgs.to_csv("ref_org_units.csv",    index=False, encoding="utf-8-sig")
        df_terrs.to_csv("ref_territories.csv", index=False, encoding="utf-8-sig")
        print(f"CSV сохранены.")

    if save_excel:
        with pd.ExcelWriter(f"{prefix}.xlsx", engine="openpyxl") as w:
            df.to_excel(w,       sheet_name="События",         index=False)
            df_orgs.to_excel(w,  sheet_name="Подразделения",   index=False)
            df_terrs.to_excel(w, sheet_name="Территории",      index=False)
        print(f"Excel сохранён: {prefix}.xlsx")

    return df, df_orgs, df_terrs


# df_events, df_orgs, df_terrs = run_full_export(save_csv=True, save_excel=True)

print("run_full_export определена.")


# %% [markdown]
# ## 8. Аналитика

# %%

def analytics_summary(df: pd.DataFrame):
    entries = df[df["is_entry"] == True]
    exits   = df[df["is_entry"] == False]
    rows = {
        "Всего событий":        len(df),
        "Входов":               len(entries),
        "Выходов":              len(exits),
        "Уникальных сотруд.":   df["person_id"].nunique(),
        "Дней в данных":        df["date"].nunique(),
        "Точек прохода":        df["territory_name"].nunique(),
        "Период с":             str(df["date"].min()),
        "Период по":            str(df["date"].max()),
    }
    for k, v in rows.items():
        print(f"  {k:<25} {v}")
    return rows


def analytics_daily_attendance(df: pd.DataFrame):
    """Первый вход и последний выход каждого сотрудника за каждый день."""
    entries = df[df["is_entry"] == True]
    exits   = df[df["is_entry"] == False]

    first_in = (
        entries.groupby(["date","person_id"])["event_datetime_local"]
        .min().reset_index().rename(columns={"event_datetime_local":"first_entry"})
    )
    last_out = (
        exits.groupby(["date","person_id"])["event_datetime_local"]
        .max().reset_index().rename(columns={"event_datetime_local":"last_exit"})
    )
    daily = pd.merge(first_in, last_out, on=["date","person_id"], how="outer")

    meta = df[["person_id","person_fullname","department"]].drop_duplicates("person_id")
    daily = daily.merge(meta, on="person_id", how="left")

    daily["time_on_site_h"] = (
        (daily["last_exit"] - daily["first_entry"]).dt.total_seconds() / 3600
    ).round(2)

    daily_unique = (
        daily.groupby("date")["person_id"].nunique()
        .reset_index().rename(columns={"person_id":"unique_visitors"})
    )
    print(f"Посещаемость: {len(daily):,} person×day")
    return daily, daily_unique


def analytics_by_department(df: pd.DataFrame) -> pd.DataFrame:
    dept = (
        df[df["is_entry"] == True]
        .groupby("department")
        .agg(
            total_entries =("event_datetime_local","count"),
            unique_persons=("person_id","nunique"),
            unique_days   =("date","nunique"),
        )
        .sort_values("total_entries", ascending=False)
        .reset_index()
    )
    return dept


def analytics_by_territory(df: pd.DataFrame) -> pd.DataFrame:
    t = (
        df.groupby(["territory_name","is_entry"]).size()
        .unstack(fill_value=0).reset_index()
    )
    t.columns.name = None
    t = t.rename(columns={True:"entries", False:"exits"})
    for col in ("entries","exits"):
        if col not in t.columns:
            t[col] = 0
    t["total"] = t["entries"] + t["exits"]
    return t.sort_values("total", ascending=False).reset_index(drop=True)


def analytics_hourly_heatmap(df: pd.DataFrame) -> pd.DataFrame:
    hm = (
        df[df["is_entry"] == True]
        .groupby(["weekday_num","hour"]).size()
        .reset_index(name="count")
    )
    DAYS = ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"]
    hm["weekday"] = hm["weekday_num"].apply(lambda x: DAYS[x] if 0<=x<=6 else "?")
    return hm


def analytics_no_exit(df: pd.DataFrame) -> pd.DataFrame:
    """Входы без парного выхода в тот же день."""
    entries = df[df["is_entry"] == True][
        ["date","person_id","person_fullname","department"]
    ].drop_duplicates(["date","person_id"])
    exits = df[df["is_entry"] == False][["date","person_id"]].copy()
    exits["has_exit"] = True

    result = entries.merge(
        exits.drop_duplicates(["date","person_id"]),
        on=["date","person_id"], how="left"
    )
    no_exit = result[result["has_exit"].isna()].drop(columns=["has_exit"])
    print(f"Входов без выхода: {len(no_exit):,}")
    return no_exit.sort_values(["date","department"])


print("Аналитические функции определены.")


# %% [markdown]
# ## 9. Пример запуска аналитики
#
# Если данные уже выгружены — загружай из файла:
# ```python
# df_events = pd.read_csv("parsec_events.csv", encoding="utf-8-sig")
# df_events["date"] = pd.to_datetime(df_events["date"]).dt.date
# df_events["event_datetime_local"] = pd.to_datetime(df_events["event_datetime_local"])
# ```

# %%

# print("=== Сводка ===")
# analytics_summary(df_events)

# print("\n=== Посещаемость ===")
# df_daily, df_daily_unique = analytics_daily_attendance(df_events)
# print(df_daily_unique.tail(10).to_string(index=False))

# print("\n=== По подразделениям ===")
# print(analytics_by_department(df_events).head(10).to_string(index=False))

# print("\n=== По точкам прохода ===")
# print(analytics_by_territory(df_events).head(10).to_string(index=False))

# print("\n=== Тепловая карта ===")
# hm = analytics_hourly_heatmap(df_events)
# print(hm.pivot(index="weekday", columns="hour", values="count").fillna(0).to_string())

# print("\n=== Аномалии ===")
# print(analytics_no_exit(df_events).head(20).to_string(index=False))

# ── Сохранить всё в один Excel ───────────────────────────────────────────────
# with pd.ExcelWriter("parsec_analytics.xlsx", engine="openpyxl") as w:
#     df_events.to_excel(w,              sheet_name="Все события",       index=False)
#     df_daily.to_excel(w,               sheet_name="Посещаемость",      index=False)
#     analytics_by_department(df_events).to_excel(w, sheet_name="По подразделениям", index=False)
#     analytics_by_territory(df_events).to_excel(w,  sheet_name="По точкам",         index=False)
#     analytics_no_exit(df_events).to_excel(w,       sheet_name="Аномалии",          index=False)

print("Раскомментируй нужные блоки для запуска.")


# %% [markdown]
# ## 10. Инкрементальная выгрузка (ежедневный крон)

# %%

def incremental_update(output_csv="parsec_events.csv", lookback_days=2):
    """
    Выгружает события за последние lookback_days дней, дозаписывает в CSV.
    lookback_days=2 — перекрытие на случай задержек в системе.
    """
    now_utc   = datetime.now(timezone.utc)
    date_from = (now_utc - timedelta(days=lookback_days)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    print(f"Инкрементальная выгрузка: {date_from.date()} → {now_utc.date()}")

    with ParsecClient(CONNECTION) as c:
        df_raw = fetch_event_history(
            c, date_from=date_from, date_to=now_utc,
            max_result_size=500_000,
        )

    if df_raw.empty:
        print("Новых событий нет.")
        return None

    df_new = postprocess_events(df_raw)
    write_header = not os.path.exists(output_csv)
    df_new.to_csv(output_csv, mode="a", header=write_header,
                  index=False, encoding="utf-8-sig")
    print(f"Добавлено {len(df_new):,} строк → {output_csv}")
    return df_new


# incremental_update(lookback_days=2)

print("incremental_update определена.")
