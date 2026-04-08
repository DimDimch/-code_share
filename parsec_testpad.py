# %% [markdown]
# # Parsec — тест-пэд
# Запускай ячейки по одной. Сессия открывается один раз вверху.

# %% [markdown]
# ## SETUP — запусти один раз в начале

# %%
import httpx
from lxml import etree
import json
from datetime import datetime, timezone, timedelta

HOST     = "192.168.1.50"
DOMAIN   = "SYSTEM"
USERNAME = "parsec"
PASSWORD = "parsec"

BASE_URL  = "http://" + HOST + ":10101/IntegrationService/IntegrationService.asmx"
NS_PARSEC = "http://parsec.ru/Parsec3IntergationService"
NS_SOAP   = "http://schemas.xmlsoap.org/soap/envelope/"

# ── низкоуровневые хелперы ────────────────────────────────────────────────────

def _envelope(method, body):
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        + '<soap:Envelope xmlns:soap="' + NS_SOAP + '"'
        + ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
        + ' xmlns:xsd="http://www.w3.org/2001/XMLSchema">'
        + '<soap:Body>'
        + '<' + method + ' xmlns="' + NS_PARSEC + '">'
        + body.strip()
        + '</' + method + '>'
        + '</soap:Body></soap:Envelope>'
    ).encode("utf-8")

def _post(method, body):
    resp = _HTTP.post(
        BASE_URL,
        content=_envelope(method, body),
        headers={
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": '"' + NS_PARSEC + "/" + method + '"',
        },
    )
    resp.raise_for_status()
    return resp

def _xml(resp):
    return etree.fromstring(resp.content)

def _text(node, tag):
    for ch in node.iter():
        if etree.QName(ch.tag).localname == tag:
            return (ch.text or "").strip()
    return None

def _all(node, tag):
    return [ch for ch in node.iter() if etree.QName(ch.tag).localname == tag]

def _pretty(resp):
    """Красивый вывод XML ответа."""
    try:
        root = etree.fromstring(resp.content)
        print(etree.tostring(root, pretty_print=True).decode())
    except Exception:
        print(resp.text)

def _sid():
    return SESSION_ID

# ── открыть сессию ────────────────────────────────────────────────────────────

_HTTP = httpx.Client(timeout=30)

resp = _post("OpenSession",
    "<domain>" + DOMAIN + "</domain>"
    + "<userName>" + USERNAME + "</userName>"
    + "<password>" + PASSWORD + "</password>"
)
root = _xml(resp)
SESSION_ID = _text(root, "SessionID")

if SESSION_ID:
    print("✓ Сессия открыта: " + SESSION_ID)
else:
    err = _text(root, "ErrorMessage")
    print("✗ Ошибка: " + str(err))
    print(resp.text[:500])


# %% [markdown]
# ---
# ## 1. Произвольный метод — сырой XML
# Укажи метод и тело запроса. Ответ выводится как pretty XML.

# %%
# ── НАСТРОЙ ЗДЕСЬ ─────────────────────────────────────────────────────────────
METHOD = "GetOrgUnitsHierarhy"
EXTRA  = ""   # дополнительные теги тела запроса, sessionID добавляется автоматически
# ──────────────────────────────────────────────────────────────────────────────

resp = _post(METHOD, "<sessionID>" + _sid() + "</sessionID>" + EXTRA)
print("HTTP:", resp.status_code)
_pretty(resp)


# %% [markdown]
# ---
# ## 2. Последние N событий — кто последний проходил

# %%
N_EVENTS = 20   # сколько последних событий показать

# Период — последние 24 часа
date_to   = datetime.now(timezone.utc)
date_from = date_to - timedelta(hours=24)

def _dt(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

# Запрашиваемые поля
FIELDS = [
    "2c5ee108-28e3-4dcc-8c95-7f3222d8e67f",  # datetime
    "57ca38e4-ed6f-4d12-adcb-2faa16f950d7",  # transaction code
    "d1847aff-11aa-4ef2-aaaa-795ceefe5f9f",  # transaction name
    "1bf8a893-7d21-4c0c-9a2d-2e333a2d769d",  # person fullname
    "633904b5-971b-4751-96a0-92dc03d5f616",  # territory name
    "0de358e0-c91b-4333-b902-000000000005",  # card code
]

fields_xml = "<fields>" + "".join("<guid>" + g + "</guid>" for g in FIELDS) + "</fields>"

# Открываем сессию истории
params = (
    "<parameters>"
    + "<StartDate>" + _dt(date_from) + "</StartDate>"
    + "<EndDate>"   + _dt(date_to)   + "</EndDate>"
    + "<MaxResultSize>1000</MaxResultSize>"
    + "</parameters>"
)
r = _post("OpenEventHistorySession",
    "<sessionID>" + _sid() + "</sessionID>" + params)
root   = _xml(r)
hist_id = _text(root, "Value")

if not hist_id:
    print("✗ Не удалось открыть сессию истории")
    print(_text(root, "ErrorMessage"))
else:
    # Количество
    r2 = _post("GetEventHistoryResultCount",
        "<sessionID>" + _sid() + "</sessionID>"
        + "<eventHistorySessionID>" + hist_id + "</eventHistorySessionID>")
    total = int(_text(_xml(r2), "GetEventHistoryResultCountResult") or "0")
    print("Всего событий за 24 часа: " + str(total))

    # Берём последние N — offset = max(0, total - N)
    offset = max(0, total - N_EVENTS)
    count  = min(N_EVENTS, total)

    r3 = _post("GetEventHistoryResult",
        "<sessionID>" + _sid() + "</sessionID>"
        + "<eventHistorySessionID>" + hist_id + "</eventHistorySessionID>"
        + fields_xml
        + "<offset>" + str(offset) + "</offset>"
        + "<count>"  + str(count)  + "</count>")

    # Парсим события
    root3   = _xml(r3)
    ev_list = [n for n in root3.iter() if etree.QName(n.tag).localname == "EventObject"]

    print("\n{:<22} {:<25} {:<30} {:<20}".format("Время", "Сотрудник", "Территория", "Тип"))
    print("-" * 100)
    for ev in ev_list:
        vals = [ch.text or "" for ch in ev.iter()
                if etree.QName(ch.tag).localname == "anyType"]
        while len(vals) < 6:
            vals.append("")
        dt_s   = vals[0][:19].replace("T", " ") if vals[0] else "—"
        tname  = vals[2][:24] if vals[2] else str(vals[1])[:24]
        person = vals[3][:29] if vals[3] else ("карта:" + vals[5])[:29]
        terr   = vals[4][:29] if vals[4] else "—"
        print("{:<22} {:<30} {:<30} {:<20}".format(dt_s, person, terr, tname))

    # Закрываем
    _post("CloseEventHistorySession",
        "<sessionID>" + _sid() + "</sessionID>"
        + "<eventHistorySessionID>" + hist_id + "</eventHistorySessionID>")
    print("\nСессия истории закрыта.")


# %% [markdown]
# ---
# ## 3. Поиск сотрудника по фамилии

# %%
SEARCH_LASTNAME  = "Иванов"
SEARCH_FIRSTNAME = ""        # можно оставить пустым
SEARCH_MIDDLE    = ""

resp = _post("FindPeople",
    "<sessionID>" + _sid() + "</sessionID>"
    + "<lastname>"   + SEARCH_LASTNAME  + "</lastname>"
    + "<firstname>"  + SEARCH_FIRSTNAME + "</firstname>"
    + "<middlename>" + SEARCH_MIDDLE    + "</middlename>"
)
root   = _xml(resp)
people = [n for n in root.iter() if etree.QName(n.tag).localname == "BasePerson"]

if not people:
    # Попробуем другой тег
    people = [n for n in root.iter() if etree.QName(n.tag).localname == "Person"]

print("Найдено: " + str(len(people)))
print()
for p in people:
    pid  = _text(p, "ID")   or _text(p, "id")
    ln   = _text(p, "LAST_NAME")
    fn   = _text(p, "FIRST_NAME")
    mn   = _text(p, "MIDDLE_NAME")
    tab  = _text(p, "TAB_NUM")
    org  = _text(p, "ORG_ID")
    print("ID:       " + str(pid))
    print("ФИО:      " + " ".join(filter(None, [ln, fn, mn])))
    print("Таб. №:   " + str(tab))
    print("Отдел ID: " + str(org))
    print()


# %% [markdown]
# ---
# ## 4. События конкретного сотрудника

# %%
# person_id из ячейки 3 или вручную
PERSON_ID = "00000000-0000-0000-0000-000000000000"   # <-- вставь сюда

HOURS_BACK = 72   # за сколько часов смотреть

date_to   = datetime.now(timezone.utc)
date_from = date_to - timedelta(hours=HOURS_BACK)

FIELDS2 = [
    "2c5ee108-28e3-4dcc-8c95-7f3222d8e67f",  # datetime
    "d1847aff-11aa-4ef2-aaaa-795ceefe5f9f",  # transaction name
    "633904b5-971b-4751-96a0-92dc03d5f616",  # territory name
]
fields_xml2 = "<fields>" + "".join("<guid>" + g + "</guid>" for g in FIELDS2) + "</fields>"

params2 = (
    "<parameters>"
    + "<StartDate>" + _dt(date_from) + "</StartDate>"
    + "<EndDate>"   + _dt(date_to)   + "</EndDate>"
    + "<Users><guid>" + PERSON_ID + "</guid></Users>"
    + "<MaxResultSize>500</MaxResultSize>"
    + "</parameters>"
)
r = _post("OpenEventHistorySession",
    "<sessionID>" + _sid() + "</sessionID>" + params2)
root    = _xml(r)
hist_id = _text(root, "Value")

if not hist_id:
    print("✗ " + str(_text(root, "ErrorMessage")))
else:
    r2    = _post("GetEventHistoryResultCount",
        "<sessionID>" + _sid() + "</sessionID>"
        + "<eventHistorySessionID>" + hist_id + "</eventHistorySessionID>")
    total = int(_text(_xml(r2), "GetEventHistoryResultCountResult") or "0")
    print("Событий за " + str(HOURS_BACK) + " часов: " + str(total))

    if total > 0:
        r3 = _post("GetEventHistoryResult",
            "<sessionID>" + _sid() + "</sessionID>"
            + "<eventHistorySessionID>" + hist_id + "</eventHistorySessionID>"
            + fields_xml2
            + "<offset>0</offset>"
            + "<count>" + str(min(total, 200)) + "</count>")
        root3   = _xml(r3)
        ev_list = [n for n in root3.iter() if etree.QName(n.tag).localname == "EventObject"]
        print()
        for ev in ev_list:
            vals = [ch.text or "" for ch in ev.iter()
                    if etree.QName(ch.tag).localname == "anyType"]
            while len(vals) < 3:
                vals.append("")
            dt_s  = vals[0][:19].replace("T", " ") if vals[0] else "—"
            tname = vals[1][:30] if vals[1] else "—"
            terr  = vals[2][:40] if vals[2] else "—"
            print(dt_s + "  |  " + tname.ljust(30) + "  |  " + terr)

    _post("CloseEventHistorySession",
        "<sessionID>" + _sid() + "</sessionID>"
        + "<eventHistorySessionID>" + hist_id + "</eventHistorySessionID>")


# %% [markdown]
# ---
# ## 5. Кто сейчас на объекте
# Считаем по принципу: последнее событие за сегодня — вход или выход.

# %%
HOURS_BACK_PRESENCE = 12   # окно анализа в часах

date_to   = datetime.now(timezone.utc)
date_from = date_to - timedelta(hours=HOURS_BACK_PRESENCE)

ENTRY_CODES = {590144, 590152}
EXIT_CODES  = {590145, 590153, 590146, 590244, 590245}

FIELDS3 = [
    "2c5ee108-28e3-4dcc-8c95-7f3222d8e67f",  # datetime
    "57ca38e4-ed6f-4d12-adcb-2faa16f950d7",  # transaction code (int)
    "7c6d82a0-c8c8-495b-9728-357807193d23",  # person_id
    "1bf8a893-7d21-4c0c-9a2d-2e333a2d769d",  # person fullname
    "633904b5-971b-4751-96a0-92dc03d5f616",  # territory name
]
fields_xml3 = "<fields>" + "".join("<guid>" + g + "</guid>" for g in FIELDS3) + "</fields>"

params3 = (
    "<parameters>"
    + "<StartDate>" + _dt(date_from) + "</StartDate>"
    + "<EndDate>"   + _dt(date_to)   + "</EndDate>"
    + "<MaxResultSize>50000</MaxResultSize>"
    + "</parameters>"
)
r = _post("OpenEventHistorySession",
    "<sessionID>" + _sid() + "</sessionID>" + params3)
root    = _xml(r)
hist_id = _text(root, "Value")

if not hist_id:
    print("✗ " + str(_text(root, "ErrorMessage")))
else:
    r2    = _post("GetEventHistoryResultCount",
        "<sessionID>" + _sid() + "</sessionID>"
        + "<eventHistorySessionID>" + hist_id + "</eventHistorySessionID>")
    total = int(_text(_xml(r2), "GetEventHistoryResultCountResult") or "0")
    print("Событий за " + str(HOURS_BACK_PRESENCE) + " часов: " + str(total))

    # Выгружаем все
    all_events = []
    chunk = 500
    for offset in range(0, total, chunk):
        n  = min(chunk, total - offset)
        r3 = _post("GetEventHistoryResult",
            "<sessionID>" + _sid() + "</sessionID>"
            + "<eventHistorySessionID>" + hist_id + "</eventHistorySessionID>"
            + fields_xml3
            + "<offset>" + str(offset) + "</offset>"
            + "<count>"  + str(n)      + "</count>")
        root3   = _xml(r3)
        ev_list = [nd for nd in root3.iter() if etree.QName(nd.tag).localname == "EventObject"]
        for ev in ev_list:
            vals = [ch.text or "" for ch in ev.iter()
                    if etree.QName(ch.tag).localname == "anyType"]
            while len(vals) < 5:
                vals.append("")
            all_events.append(vals)

    _post("CloseEventHistorySession",
        "<sessionID>" + _sid() + "</sessionID>"
        + "<eventHistorySessionID>" + hist_id + "</eventHistorySessionID>")

    # Для каждого person_id — последнее событие
    last = {}   # person_id -> (dt_str, code, name, territory)
    for vals in all_events:
        pid = vals[2]
        if not pid:
            continue
        dt_s  = vals[0]
        try:
            code = int(vals[1])
        except Exception:
            code = 0
        name  = vals[3]
        terr  = vals[4]
        if pid not in last or dt_s > last[pid][0]:
            last[pid] = (dt_s, code, name, terr)

    on_site  = [(v[2], v[0][:19].replace("T"," "), v[3]) for v in last.values() if v[1] in ENTRY_CODES]
    off_site = [(v[2], v[0][:19].replace("T"," "), v[3]) for v in last.values() if v[1] in EXIT_CODES]

    on_site.sort(key=lambda x: x[1], reverse=True)

    print("\n=== НА ОБЪЕКТЕ: " + str(len(on_site)) + " чел. ===")
    print("{:<35} {:<22} {}".format("Сотрудник", "Последний вход", "Точка прохода"))
    print("-" * 90)
    for name, dt_s, terr in on_site:
        print("{:<35} {:<22} {}".format(
            (name or "—")[:34], dt_s, (terr or "—")[:30]
        ))

    print("\n=== ПОКИНУЛИ ОБЪЕКТ: " + str(len(off_site)) + " чел. ===")


# %% [markdown]
# ---
# ## 6. Список всех территорий (точек прохода)

# %%
resp   = _post("GetTerritoriesHierarhy",
    "<sessionID>" + _sid() + "</sessionID>")
root   = _xml(resp)
terrs  = [n for n in root.iter() if etree.QName(n.tag).localname in ("Territory", "TerritoryWithComponent", "BaseTerritory")]

# Фильтр по типу — только двери (type=1), убери если нужно всё
TYPE_FILTER = None   # None = все, 1 = только двери

print("{:<38} {:<5} {}".format("ID", "Тип", "Название"))
print("-" * 90)
for t in terrs:
    tid   = _text(t, "ID")   or "—"
    ttype = _text(t, "TYPE") or "?"
    tname = _text(t, "NAME") or "—"
    if TYPE_FILTER is not None and str(ttype) != str(TYPE_FILTER):
        continue
    print("{:<38} {:<5} {}".format(tid, ttype, tname))

print("\nВсего: " + str(len(terrs)))


# %% [markdown]
# ---
# ## 7. События по конкретной точке прохода

# %%
TERRITORY_ID = "00000000-0000-0000-0000-000000000000"  # <-- ID из ячейки 6
HOURS_BACK_T = 24

date_to   = datetime.now(timezone.utc)
date_from = date_to - timedelta(hours=HOURS_BACK_T)

FIELDS4 = [
    "2c5ee108-28e3-4dcc-8c95-7f3222d8e67f",  # datetime
    "d1847aff-11aa-4ef2-aaaa-795ceefe5f9f",  # transaction name
    "1bf8a893-7d21-4c0c-9a2d-2e333a2d769d",  # person fullname
    "0de358e0-c91b-4333-b902-000000000005",  # card code
]
fields_xml4 = "<fields>" + "".join("<guid>" + g + "</guid>" for g in FIELDS4) + "</fields>"

params4 = (
    "<parameters>"
    + "<StartDate>" + _dt(date_from) + "</StartDate>"
    + "<EndDate>"   + _dt(date_to)   + "</EndDate>"
    + "<Territories><guid>" + TERRITORY_ID + "</guid></Territories>"
    + "<MaxResultSize>1000</MaxResultSize>"
    + "</parameters>"
)
r = _post("OpenEventHistorySession",
    "<sessionID>" + _sid() + "</sessionID>" + params4)
root    = _xml(r)
hist_id = _text(root, "Value")

if not hist_id:
    print("✗ " + str(_text(root, "ErrorMessage")))
else:
    r2    = _post("GetEventHistoryResultCount",
        "<sessionID>" + _sid() + "</sessionID>"
        + "<eventHistorySessionID>" + hist_id + "</eventHistorySessionID>")
    total = int(_text(_xml(r2), "GetEventHistoryResultCountResult") or "0")
    print("Событий на точке за " + str(HOURS_BACK_T) + " часов: " + str(total))

    if total > 0:
        r3 = _post("GetEventHistoryResult",
            "<sessionID>" + _sid() + "</sessionID>"
            + "<eventHistorySessionID>" + hist_id + "</eventHistorySessionID>"
            + fields_xml4
            + "<offset>0</offset>"
            + "<count>" + str(min(total, 200)) + "</count>")
        root3   = _xml(r3)
        ev_list = [n for n in root3.iter() if etree.QName(n.tag).localname == "EventObject"]
        print()
        for ev in ev_list:
            vals = [ch.text or "" for ch in ev.iter()
                    if etree.QName(ch.tag).localname == "anyType"]
            while len(vals) < 4:
                vals.append("")
            dt_s   = vals[0][:19].replace("T", " ") if vals[0] else "—"
            tname  = vals[1][:25] if vals[1] else "—"
            person = vals[2][:30] if vals[2] else ("карта:" + vals[3])[:30]
            print(dt_s + "  " + tname.ljust(26) + "  " + person)

    _post("CloseEventHistorySession",
        "<sessionID>" + _sid() + "</sessionID>"
        + "<eventHistorySessionID>" + hist_id + "</eventHistorySessionID>")


# %% [markdown]
# ---
# ## 8. Версия и информация о сервисе

# %%
resp = _post("GetVersion", "")
print("Версия API: " + (_text(_xml(resp), "GetVersionResult") or "—"))

resp2 = _post("GetDomains", "")
root2 = _xml(resp2)
domains = [n for n in root2.iter() if etree.QName(n.tag).localname == "Domain"]
print("Организации:")
for d in domains:
    name    = _text(d, "NAME") or "—"
    is_sys  = _text(d, "IS_SYSTEM") or "false"
    print("  " + name + (" [SYSTEM]" if is_sys == "true" else ""))


# %% [markdown]
# ---
# ## CLOSE — закрыть сессию

# %%
_post("CloseSession", "<sessionID>" + _sid() + "</sessionID>")
_HTTP.close()
print("Сессия закрыта.")
