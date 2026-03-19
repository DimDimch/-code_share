# %% [markdown]
# # Parsec ACS — выгрузка данных пропускного режима
# Полный пайплайн: сессия → справочники → события → DataFrame → базовая аналитика

# %% [markdown]
# ## 0. Установка зависимостей

# %%
# pip install zeep pandas openpyxl pytz
# (раскомментируй и запусти один раз)
# import subprocess
# subprocess.run(["pip", "install", "zeep", "pandas", "openpyxl", "pytz"])


# %% [markdown]
# ## 1. Конфигурация подключения

# %%
import json
import pathlib
from datetime import datetime, timezone, timedelta

# ── Параметры подключения ──────────────────────────────────────────────────────
CONNECTION = {
    "host_addr": "127.0.0.1",          # IP или hostname ParsecNET-сервера
    "organization": "SYSTEM",           # организация (домен)
    "username": "parsec",
    "password": "parsec",
    "wsdl_template": "http://{}:10101/IntegrationService/IntegrationService.asmx?WSDL",
}

# ── Период выгрузки (UTC) ──────────────────────────────────────────────────────
DATE_FROM = datetime(2024,  1,  1, tzinfo=timezone.utc)
DATE_TO   = datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

# ── Типы транзакций для пропускного режима ────────────────────────────────────
#   590144 — нормальный вход по ключу
#   590145 — нормальный выход по ключу
#   590152 — фактический вход
#   590153 — фактический выход
#   590146 — выход вне временного профиля
#   590244 — нормальный выход посетителя
#   590245 — фактический выход посетителя
TRANSACTION_TYPES = [590144, 590145, 590152, 590153, 590146, 590244, 590245]

# ── Максимальное кол-во событий (без явного указания API вернёт только 5000!) ──
MAX_RESULT_SIZE = 10_000_000

# ── Размер чанка при постраничной выгрузке ────────────────────────────────────
CHUNK_SIZE = 500

# ── Локальная временная зона для итоговых данных ──────────────────────────────
LOCAL_TZ_OFFSET_HOURS = 3   # UTC+3 (Москва)

# ── Поля событий, которые мы запрашиваем (GUID → название столбца) ────────────
#   Порядок важен! Values в EventObject возвращаются строго в этом порядке.
EVENT_FIELDS = {
    "2c5ee108-28e3-4dcc-8c95-7f3222d8e67f": "event_datetime",   # дата+время
    "57ca38e4-ed6f-4d12-adcb-2faa16f950d7": "transaction_code", # тип (10-digit)
    "d1847aff-11aa-4ef2-aaaa-795ceefe5f9f": "transaction_name", # тип (название)
    "633904b5-971b-4751-96a0-92dc03d5f616": "territory_name",   # точка прохода
    "4c5807cb-2c06-4725-9243-747e40c41d6c": "zone_name",        # зона
    "7c6d82a0-c8c8-495b-9728-357807193d23": "person_id",        # PERS_ID (Guid)
    "1bf8a893-7d21-4c0c-9a2d-2e333a2d769d": "person_fullname",  # ФИО
    "0de358e0-c91b-4333-b902-000000000003": "person_lastname",  # Фамилия
    "0de358e0-c91b-4333-b902-000000000001": "person_firstname",  # Имя
    "0de358e0-c91b-4333-b902-000000000004": "department",       # Подразделение
    "0de358e0-c91b-4333-b902-000000000006": "tab_number",       # Табельный номер
    "0de358e0-c91b-4333-b902-000000000005": "card_code",        # Код карты
}

FIELD_GUIDS   = list(EVENT_FIELDS.keys())
FIELD_COLUMNS = list(EVENT_FIELDS.values())

print("Конфигурация загружена.")
print(f"Период: {DATE_FROM.date()} — {DATE_TO.date()}")
print(f"Типов транзакций: {len(TRANSACTION_TYPES)}")
print(f"Полей событий:    {len(FIELD_GUIDS)}")


# %% [markdown]
# ## 2. Класс сессии (обёртка над zeep)

# %%
import zeep
import zeep.exceptions


class ParsecSession:
    """
    Контекстный менеджер для сессии Parsec Integration Service.

    Использование:
        with ParsecSession(CONNECTION) as ps:
            orgs = ps.call("GetOrgUnitsHierarhy")
    """

    def __init__(self, config: dict):
        self.config = config
        self._client = None
        self._session_id = None
        self._namespace = None

    # ── внутренние хелперы ────────────────────────────────────────────────────

    def _resolve_namespace(self):
        for key, value in self._client.namespaces.items():
            if "Parsec3IntergationService" in value or "ParsecIntergationService" in value:
                self._namespace = key
                return
        raise RuntimeError("Не удалось определить namespace Parsec в WSDL")

    def get_type(self, type_name: str):
        if self._namespace is None:
            self._resolve_namespace()
        try:
            return self._client.get_type(f"{self._namespace}:{type_name}")
        except Exception as e:
            raise RuntimeError(f"Тип '{type_name}' не найден в WSDL: {e}")

    def make(self, type_name: str):
        """Создать экземпляр WSDL-типа."""
        return self.get_type(type_name)()

    # ── подключение / отключение ──────────────────────────────────────────────

    def connect(self):
        wsdl_url = self.config["wsdl_template"].format(self.config["host_addr"])
        print(f"Подключение к {wsdl_url} ...")
        self._client = zeep.Client(wsdl_url)

        result = self._client.service.OpenSession(
            self.config["organization"],
            self.config["username"],
            self.config["password"],
        )
        if result.Result == -1:
            raise ConnectionError(f"Ошибка авторизации: {result.ErrorMessage}")

        self._session_id = result.Value.SessionID
        print(f"Сессия открыта. SessionID: {self._session_id}")
        return self

    def disconnect(self):
        if self._session_id and self._client:
            try:
                self._client.service.CloseSession(self._session_id)
                print("Сессия закрыта.")
            except Exception as e:
                print(f"Предупреждение при закрытии сессии: {e}")
        self._session_id = None

    def __enter__(self):
        return self.connect()

    def __exit__(self, *_):
        self.disconnect()

    # ── универсальный вызов сервиса ───────────────────────────────────────────

    def call(self, method_name: str, *args, inject_session=True):
        """
        Вызвать метод сервиса.
        Если inject_session=True, автоматически подставляет sessionId первым аргументом.
        """
        svc = self._client.service
        if not hasattr(svc, method_name):
            raise AttributeError(f"Метод '{method_name}' не найден в WSDL")
        fn = getattr(svc, method_name)
        if inject_session:
            return fn(self._session_id, *args)
        return fn(*args)

    @property
    def session_id(self):
        return self._session_id


print("Класс ParsecSession определён.")


# %% [markdown]
# ## 3. Загрузка справочников

# %%
import pandas as pd


def load_org_units(ps: ParsecSession) -> pd.DataFrame:
    """Возвращает DataFrame со всеми подразделениями."""
    print("Загрузка дерева подразделений...")
    units = ps.call("GetOrgUnitsHierarhy")
    if not units:
        return pd.DataFrame(columns=["id", "name", "desc", "parent_id"])
    rows = []
    for u in units:
        rows.append({
            "id":        str(u.ID),
            "name":      u.NAME,
            "desc":      getattr(u, "DESC", None),
            "parent_id": str(getattr(u, "PARENT_ID", "")),
        })
    df = pd.DataFrame(rows)
    print(f"  → {len(df)} подразделений")
    return df


def load_territories(ps: ParsecSession) -> pd.DataFrame:
    """Возвращает DataFrame со всеми точками прохода / зонами."""
    print("Загрузка дерева территорий...")
    terrs = ps.call("GetTerritoriesHierarhy")
    if not terrs:
        return pd.DataFrame(columns=["id", "type", "name", "desc", "parent_id"])
    rows = []
    for t in terrs:
        rows.append({
            "id":        str(t.ID),
            "type":      t.TYPE,   # 0=папка, 1=дверь, 3=другое
            "name":      t.NAME,
            "desc":      getattr(t, "DESC", None),
            "parent_id": str(getattr(t, "PARENT_ID", "")),
        })
    df = pd.DataFrame(rows)
    print(f"  → {len(df)} территорий")
    return df


def load_transaction_types(ps: ParsecSession) -> pd.DataFrame:
    """Возвращает DataFrame всех типов событий системы."""
    print("Загрузка типов транзакций...")
    # classMask = -1 → все категории (64-bit signed -1 = все биты = 1)
    types = ps.call("GetTransactionTypes", -1)
    if not types:
        return pd.DataFrame(columns=["id", "name", "class_mask"])
    rows = [{"id": t.ID, "name": t.NAME, "class_mask": t.CLASS_MASK} for t in types]
    df = pd.DataFrame(rows)
    print(f"  → {len(df)} типов транзакций")
    return df


# ── Запуск ────────────────────────────────────────────────────────────────────
# Раскомментируй блок ниже для реального подключения

# with ParsecSession(CONNECTION) as ps:
#     df_orgs   = load_org_units(ps)
#     df_terrs  = load_territories(ps)
#     df_ttypes = load_transaction_types(ps)

# df_orgs.to_csv("ref_org_units.csv", index=False, encoding="utf-8-sig")
# df_terrs.to_csv("ref_territories.csv", index=False, encoding="utf-8-sig")
# df_ttypes.to_csv("ref_transaction_types.csv", index=False, encoding="utf-8-sig")

print("Функции загрузки справочников определены.")


# %% [markdown]
# ## 4. Выгрузка исторических событий

# %%
from typing import Optional
import time


def build_event_history_params(ps: ParsecSession,
                                date_from: datetime,
                                date_to: datetime,
                                transaction_types: list,
                                max_result_size: int,
                                territory_ids: Optional[list] = None,
                                org_ids: Optional[list] = None,
                                person_ids: Optional[list] = None):
    """
    Собирает объект EventHistoryQueryParams для передачи в OpenEventHistorySession.
    Все datetime должны быть в UTC.
    """
    params = ps.make("EventHistoryQueryParams")

    # Временной диапазон (UTC)
    params.StartDate = date_from
    params.EndDate   = date_to

    # Типы транзакций
    arr_type = ps.get_type("ArrayOfUnsignedInt")
    tt_arr = arr_type()
    tt_arr.unsignedInt = transaction_types
    params.TransactionTypes = tt_arr

    # Максимум записей — ОБЯЗАТЕЛЬНО указывать явно, иначе вернёт только 5000
    params.MaxResultSize = max_result_size

    # Опциональные фильтры
    if territory_ids:
        arr_guid = ps.get_type("ArrayOfGuid")
        g = arr_guid()
        g.guid = territory_ids
        params.Territories = g

    if org_ids:
        arr_guid = ps.get_type("ArrayOfGuid")
        g = arr_guid()
        g.guid = org_ids
        params.Organizations = g

    if person_ids:
        arr_guid = ps.get_type("ArrayOfGuid")
        g = arr_guid()
        g.guid = person_ids
        params.Users = g

    return params


def fetch_event_history(ps: ParsecSession,
                         date_from: datetime,
                         date_to: datetime,
                         transaction_types: list = TRANSACTION_TYPES,
                         field_guids: list = FIELD_GUIDS,
                         field_columns: list = FIELD_COLUMNS,
                         max_result_size: int = MAX_RESULT_SIZE,
                         chunk_size: int = CHUNK_SIZE,
                         territory_ids: Optional[list] = None,
                         org_ids: Optional[list] = None,
                         person_ids: Optional[list] = None) -> pd.DataFrame:
    """
    Выгружает историю событий и возвращает pandas DataFrame.

    Параметры:
        ps               — активная сессия ParsecSession
        date_from/to     — период в UTC
        transaction_types — список кодов транзакций
        field_guids      — список GUID полей для выгрузки
        field_columns    — имена столбцов в том же порядке
        chunk_size       — размер страницы при выгрузке
        territory_ids    — список GUID территорий (None = все)
        org_ids          — список GUID подразделений (None = все)
        person_ids       — список GUID сотрудников (None = все)
    """

    # 1. Открываем сессию истории событий
    print("Открытие сессии истории событий...")
    params = build_event_history_params(
        ps, date_from, date_to, transaction_types, max_result_size,
        territory_ids, org_ids, person_ids
    )
    hist_result = ps.call("OpenEventHistorySession", params)
    if hist_result.Result == -1:
        raise RuntimeError(f"OpenEventHistorySession: {hist_result.ErrorMessage}")
    hist_session_id = hist_result.Value
    print(f"  Сессия истории: {hist_session_id}")

    try:
        # 2. Узнаём общее количество событий
        total = ps.call("GetEventHistoryResultCount", hist_session_id)
        print(f"  Всего событий в выборке: {total:,}")
        if total == 0:
            return pd.DataFrame(columns=field_columns)

        # 3. Список GUID полей для API
        arr_guid = ps.get_type("ArrayOfGuid")
        fields_obj = arr_guid()
        fields_obj.guid = field_guids

        # 4. Постраничная выгрузка
        all_rows = []
        t0 = time.time()

        for offset in range(0, total, chunk_size):
            count = min(chunk_size, total - offset)
            events = ps.call(
                "GetEventHistoryResult",
                hist_session_id,
                fields_obj,
                offset,
                count
            )
            if events:
                for ev in events:
                    # Values — массив строк строго в порядке field_guids
                    values = list(ev.Values.anyType) if ev.Values else [None] * len(field_columns)
                    # Дополняем None если полей вернулось меньше (защита)
                    while len(values) < len(field_columns):
                        values.append(None)
                    all_rows.append(values[:len(field_columns)])

            # Прогресс
            elapsed = time.time() - t0
            pct = min((offset + count) / total * 100, 100)
            speed = (offset + count) / elapsed if elapsed > 0 else 0
            eta = (total - offset - count) / speed if speed > 0 else 0
            print(f"\r  {pct:5.1f}%  {offset+count:>{len(str(total))}}/{total}"
                  f"  {speed:.0f} зап/с  ETA {eta:.0f}с   ", end="", flush=True)

        print(f"\n  Выгрузка завершена за {time.time()-t0:.1f}с")

    finally:
        # 5. Закрываем сессию истории — ВСЕГДА
        ps.call("CloseEventHistorySession", hist_session_id)
        print("  Сессия истории закрыта.")

    # 6. Собираем DataFrame
    df = pd.DataFrame(all_rows, columns=field_columns)
    print(f"  DataFrame: {df.shape[0]:,} строк × {df.shape[1]} столбцов")
    return df


print("Функция fetch_event_history определена.")


# %% [markdown]
# ## 5. Постобработка DataFrame

# %%

def postprocess_events(df: pd.DataFrame,
                        local_tz_offset_hours: int = LOCAL_TZ_OFFSET_HOURS) -> pd.DataFrame:
    """
    Приводит сырой DataFrame событий к удобному для аналитики виду:
      - парсит datetime, переводит в локальный часовой пояс
      - добавляет колонки date, hour, weekday, is_entry
      - чистит строковые поля
    """
    if df.empty:
        return df

    df = df.copy()

    # ── datetime ──────────────────────────────────────────────────────────────
    # API возвращает строку вида '2024-03-15T08:30:00' или уже datetime
    def parse_dt(val):
        if val is None:
            return pd.NaT
        if isinstance(val, datetime):
            return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
        try:
            dt = pd.to_datetime(val)
            if dt.tzinfo is None:
                dt = dt.tz_localize("UTC")
            return dt
        except Exception:
            return pd.NaT

    df["event_datetime"] = df["event_datetime"].apply(parse_dt)

    # Локальное время (для вывода и аналитики)
    local_tz = timezone(timedelta(hours=local_tz_offset_hours))
    df["event_datetime_local"] = df["event_datetime"].apply(
        lambda x: x.astimezone(local_tz) if pd.notna(x) else pd.NaT
    )

    # Производные колонки
    df["date"]    = df["event_datetime_local"].apply(lambda x: x.date() if pd.notna(x) else None)
    df["hour"]    = df["event_datetime_local"].apply(lambda x: x.hour  if pd.notna(x) else None)
    df["weekday"] = df["event_datetime_local"].apply(
        lambda x: x.strftime("%A") if pd.notna(x) else None
    )
    df["weekday_num"] = df["event_datetime_local"].apply(
        lambda x: x.weekday() if pd.notna(x) else None   # 0=пн, 6=вс
    )

    # ── тип прохода ───────────────────────────────────────────────────────────
    ENTRY_CODES = {590144, 590152}    # вход (нормальный + фактический)
    EXIT_CODES  = {590145, 590153, 590146, 590244, 590245}

    def parse_code(val):
        try:
            return int(val)
        except (TypeError, ValueError):
            return None

    df["transaction_code"] = df["transaction_code"].apply(parse_code)
    df["is_entry"] = df["transaction_code"].apply(
        lambda c: True if c in ENTRY_CODES else (False if c in EXIT_CODES else None)
    )

    # ── строковые поля ────────────────────────────────────────────────────────
    str_cols = ["person_fullname", "person_lastname", "person_firstname",
                "department", "territory_name", "zone_name",
                "tab_number", "card_code", "transaction_name"]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace("None", None)

    # ── упорядочиваем столбцы ─────────────────────────────────────────────────
    priority = ["event_datetime_local", "date", "hour", "weekday",
                "is_entry", "transaction_code", "transaction_name",
                "person_fullname", "department", "tab_number",
                "territory_name", "zone_name", "card_code", "person_id",
                "event_datetime"]
    rest = [c for c in df.columns if c not in priority]
    df = df[[c for c in priority if c in df.columns] + rest]

    return df


print("Функция postprocess_events определена.")


# %% [markdown]
# ## 6. Основной запуск — выгрузка и сохранение

# %%

def run_full_export(save_csv: bool = True,
                    save_excel: bool = False,
                    output_prefix: str = "parsec_events"):
    """
    Полный цикл: подключение → выгрузка → постобработка → сохранение.
    """
    with ParsecSession(CONNECTION) as ps:

        # Справочники
        df_orgs  = load_org_units(ps)
        df_terrs = load_territories(ps)

        # События
        df_raw = fetch_event_history(
            ps,
            date_from=DATE_FROM,
            date_to=DATE_TO,
            transaction_types=TRANSACTION_TYPES,
            field_guids=FIELD_GUIDS,
            field_columns=FIELD_COLUMNS,
            max_result_size=MAX_RESULT_SIZE,
            chunk_size=CHUNK_SIZE,
        )

    # Постобработка (уже вне сессии — не тратим время соединения)
    df = postprocess_events(df_raw)

    print(f"\nИтог: {len(df):,} событий")

    # Сохранение
    if save_csv:
        path = f"{output_prefix}.csv"
        df.to_csv(path, index=False, encoding="utf-8-sig")
        print(f"Сохранено: {path}")

        df_orgs.to_csv("ref_org_units.csv",   index=False, encoding="utf-8-sig")
        df_terrs.to_csv("ref_territories.csv", index=False, encoding="utf-8-sig")

    if save_excel:
        path = f"{output_prefix}.xlsx"
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="События",         index=False)
            df_orgs.to_excel(writer, sheet_name="Подразделения", index=False)
            df_terrs.to_excel(writer, sheet_name="Территории",   index=False)
        print(f"Сохранено: {path}")

    return df, df_orgs, df_terrs


# ── Раскомментируй для запуска ────────────────────────────────────────────────
# df_events, df_orgs, df_terrs = run_full_export(save_csv=True, save_excel=True)

print("Функция run_full_export определена. Раскомментируй вызов для запуска.")


# %% [markdown]
# ## 7. Базовая аналитика пропускного режима
#
# Этот раздел работает с уже выгруженным `df_events`.
# Если перезапускаешь ноутбук без подключения — загружай из CSV:
# ```python
# df_events = pd.read_csv("parsec_events.csv", parse_dates=["event_datetime_local"])
# ```

# %%

def analytics_summary(df: pd.DataFrame) -> dict:
    """Базовые агрегаты по датасету событий."""
    if df.empty:
        print("DataFrame пуст.")
        return {}

    entries = df[df["is_entry"] == True]
    exits   = df[df["is_entry"] == False]

    summary = {
        "total_events":     len(df),
        "total_entries":    len(entries),
        "total_exits":      len(exits),
        "unique_persons":   df["person_id"].nunique(),
        "unique_dates":     df["date"].nunique(),
        "unique_territories": df["territory_name"].nunique(),
        "date_min":         df["date"].min(),
        "date_max":         df["date"].max(),
    }

    for k, v in summary.items():
        print(f"  {k:<25} {v}")

    return summary


def analytics_daily_attendance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Посещаемость по дням: количество уникальных сотрудников,
    первый вход и последний выход по каждому человеку.
    """
    entries = df[df["is_entry"] == True].copy()
    exits   = df[df["is_entry"] == False].copy()

    # Первый вход за день
    first_entry = (
        entries.groupby(["date", "person_id"])["event_datetime_local"]
        .min()
        .reset_index()
        .rename(columns={"event_datetime_local": "first_entry"})
    )
    # Последний выход за день
    last_exit = (
        exits.groupby(["date", "person_id"])["event_datetime_local"]
        .max()
        .reset_index()
        .rename(columns={"event_datetime_local": "last_exit"})
    )

    # Объединяем
    daily = pd.merge(first_entry, last_exit, on=["date", "person_id"], how="outer")

    # Добавляем ФИО и подразделение
    meta = (
        df[["person_id", "person_fullname", "department"]]
        .drop_duplicates("person_id")
    )
    daily = daily.merge(meta, on="person_id", how="left")

    # Время на объекте
    daily["time_on_site_h"] = (
        (daily["last_exit"] - daily["first_entry"])
        .dt.total_seconds() / 3600
    ).round(2)

    # Уникальных посетителей за день
    daily_unique = (
        daily.groupby("date")["person_id"]
        .nunique()
        .reset_index()
        .rename(columns={"person_id": "unique_visitors"})
    )

    print(f"Посещаемость: {len(daily):,} записей (person × day)")
    return daily, daily_unique


def analytics_by_department(df: pd.DataFrame) -> pd.DataFrame:
    """Активность по подразделениям."""
    dept = (
        df[df["is_entry"] == True]
        .groupby("department")
        .agg(
            total_entries=("event_datetime_local", "count"),
            unique_persons=("person_id", "nunique"),
            unique_days=("date", "nunique"),
        )
        .sort_values("total_entries", ascending=False)
        .reset_index()
    )
    print(f"Подразделений с активностью: {len(dept)}")
    return dept


def analytics_by_territory(df: pd.DataFrame) -> pd.DataFrame:
    """Нагрузка по точкам прохода."""
    terr = (
        df.groupby(["territory_name", "is_entry"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )
    terr.columns.name = None
    terr = terr.rename(columns={True: "entries", False: "exits"})
    if "entries" not in terr.columns:
        terr["entries"] = 0
    if "exits" not in terr.columns:
        terr["exits"] = 0
    terr["total"] = terr["entries"] + terr["exits"]
    terr = terr.sort_values("total", ascending=False).reset_index(drop=True)
    print(f"Точек прохода: {len(terr)}")
    return terr


def analytics_hourly_heatmap(df: pd.DataFrame) -> pd.DataFrame:
    """
    Тепловая карта: час × день_недели → количество событий.
    Удобно для визуализации в Power BI / Seaborn.
    """
    heatmap = (
        df[df["is_entry"] == True]
        .groupby(["weekday_num", "hour"])
        .size()
        .reset_index(name="count")
    )
    DAYS = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    heatmap["weekday_name"] = heatmap["weekday_num"].apply(
        lambda x: DAYS[x] if 0 <= x <= 6 else "?"
    )
    return heatmap


def analytics_no_exit(df: pd.DataFrame) -> pd.DataFrame:
    """
    Аномалии: сотрудники, у которых есть вход, но нет выхода в тот же день.
    Возможные причины: турникет не зафиксировал выход, другая точка выхода,
    остался работать ночью.
    """
    entries = df[df["is_entry"] == True][["date", "person_id", "person_fullname", "department"]].copy()
    exits   = df[df["is_entry"] == False][["date", "person_id"]].copy()
    exits["has_exit"] = True

    merged = entries.drop_duplicates(["date", "person_id"]).merge(
        exits.drop_duplicates(["date", "person_id"]),
        on=["date", "person_id"],
        how="left"
    )
    no_exit = merged[merged["has_exit"].isna()].drop(columns=["has_exit"])
    print(f"Записей 'вход без выхода': {len(no_exit):,}")
    return no_exit.sort_values(["date", "department"])


print("Аналитические функции определены.")


# %% [markdown]
# ## 8. Запуск аналитики

# %%

# ── Если выгружал ранее — загружай из файла, не нужно переподключаться ─────────
# df_events = pd.read_csv("parsec_events.csv", encoding="utf-8-sig")
# df_events["date"] = pd.to_datetime(df_events["date"]).dt.date
# df_events["event_datetime_local"] = pd.to_datetime(df_events["event_datetime_local"])

# ── Пример использования (раскомментируй после выгрузки) ─────────────────────

# print("=== Общая сводка ===")
# summary = analytics_summary(df_events)

# print("\n=== Посещаемость по дням ===")
# df_daily, df_daily_unique = analytics_daily_attendance(df_events)
# print(df_daily_unique.head(10))

# print("\n=== По подразделениям ===")
# df_dept = analytics_by_department(df_events)
# print(df_dept.head(10))

# print("\n=== По точкам прохода ===")
# df_terr_load = analytics_by_territory(df_events)
# print(df_terr_load.head(10))

# print("\n=== Тепловая карта (час × день) ===")
# df_heatmap = analytics_hourly_heatmap(df_events)
# print(df_heatmap.pivot(index="weekday_name", columns="hour", values="count").fillna(0))

# print("\n=== Аномалии: вход без выхода ===")
# df_no_exit = analytics_no_exit(df_events)
# print(df_no_exit.head(20))

# ── Сохранение аналитических таблиц ──────────────────────────────────────────
# with pd.ExcelWriter("parsec_analytics.xlsx", engine="openpyxl") as w:
#     df_events.to_excel(w, sheet_name="Все события",     index=False)
#     df_daily.to_excel(w,  sheet_name="Посещаемость",    index=False)
#     df_dept.to_excel(w,   sheet_name="По подразделениям", index=False)
#     df_terr_load.to_excel(w, sheet_name="По точкам",    index=False)
#     df_no_exit.to_excel(w, sheet_name="Аномалии",       index=False)
# print("Аналитика сохранена в parsec_analytics.xlsx")

print("Ячейка готова. Раскомментируй нужные блоки для запуска аналитики.")


# %% [markdown]
# ## 9. Инкрементальная выгрузка (ежедневное обновление)
#
# Для production-использования — запускай ежедневно, выгружая только вчерашние события
# и дозаписывая в общий файл.

# %%
import os


def incremental_update(output_csv: str = "parsec_events.csv",
                        lookback_days: int = 1):
    """
    Выгружает события за последние `lookback_days` дней и дозаписывает в CSV.
    Запускай по крону или Task Scheduler.

    lookback_days=2 даёт перекрытие на случай задержек в системе.
    """
    now_utc = datetime.now(timezone.utc)
    date_from = (now_utc - timedelta(days=lookback_days)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    date_to = now_utc

    print(f"Инкрементальная выгрузка: {date_from} → {date_to}")

    with ParsecSession(CONNECTION) as ps:
        df_raw = fetch_event_history(
            ps,
            date_from=date_from,
            date_to=date_to,
            transaction_types=TRANSACTION_TYPES,
            field_guids=FIELD_GUIDS,
            field_columns=FIELD_COLUMNS,
            max_result_size=500_000,
            chunk_size=CHUNK_SIZE,
        )

    if df_raw.empty:
        print("Новых событий нет.")
        return

    df_new = postprocess_events(df_raw)

    # Дозапись (или создание нового файла)
    write_header = not os.path.exists(output_csv)
    df_new.to_csv(output_csv, mode="a", header=write_header,
                  index=False, encoding="utf-8-sig")

    print(f"Добавлено {len(df_new):,} строк в {output_csv}")
    return df_new


# ── Раскомментируй для ежедневного запуска ────────────────────────────────────
# incremental_update(output_csv="parsec_events.csv", lookback_days=2)

print("Функция incremental_update определена.")
