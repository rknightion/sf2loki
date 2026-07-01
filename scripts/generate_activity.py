#!/usr/bin/env python3
"""Synthetic Salesforce activity generator — makes a dev org look busy and real.

Drives constant, low-level, randomised activity through the Salesforce REST API
so that EventLogFile (ELF) ingestion and the streaming pipeline have realistic
data to chew on. Covers the breadth of event categories an org normally sees:

  * object CRUD (Account, Contact, Lead, Opportunity, Case, Task, Event)
    -> API / RestApi ELF events, and the underlying records product queries hit
  * SOQL queries                       -> API ELF events
  * SOSL searches                      -> Search ELF events
  * report runs (existing reports)     -> Report ELF events
  * Bulk API 2.0 ingest jobs           -> BulkApi ELF events
  * anonymous Apex (Tooling API)       -> ApexExecution ELF events
  * periodic re-authentication         -> Login ELF events + LoginEventStream

It authenticates with the same JWT-bearer credentials sf2loki uses (read from a
`.env` file or the environment), stamps every record it creates with a marker in
the Description field, and can clean them all up again with `--cleanup`.

Cannot be generated via API (documented limitation): URI / LightningPageView /
LightningInteraction ELF types require a real browser UI session.

Usage
-----
  # continuous low-level noise (~6 ops/min) until Ctrl-C:
  python scripts/generate_activity.py --env-file .env.dev

  # heavier burst for an hour:
  python scripts/generate_activity.py --env-file .env.dev --ops-per-min 30 --duration 3600

  # delete everything this tool ever created:
  python scripts/generate_activity.py --env-file .env.dev --cleanup

This is a dev/test utility. It only ever touches the org named by SF_LOGIN_URL.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import os
import random
import signal
import string
import sys
import urllib.parse
from collections import defaultdict, deque
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import jwt

log = logging.getLogger("gen_activity")

# Marker stamped into every created record's Description so --cleanup can find
# them. Chosen to be obvious in the UI and unlikely to collide with real data.
MARKER = "[sf2loki-synthetic]"

# JWT assertion lifetime — Salesforce rejects exp > 3 minutes out.
_JWT_LIFETIME = timedelta(seconds=180)

# ---------------------------------------------------------------------------
# Realistic-ish data pools
# ---------------------------------------------------------------------------

_FIRST_NAMES = [
    "Olivia", "Liam", "Emma", "Noah", "Ava", "Ethan", "Sophia", "Mason",
    "Isabella", "Lucas", "Mia", "Oliver", "Amelia", "Elijah", "Harper",
    "James", "Evelyn", "Benjamin", "Abigail", "Henry", "Priya", "Aarav",
    "Wei", "Fatima", "Diego", "Yuki", "Kwame", "Ingrid", "Mateo", "Aisha",
]
_LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Nguyen", "Patel", "Kim", "Okafor", "Rossi",
    "Andersson", "Tanaka", "Muller", "Silva", "Haddad", "Novak", "Petrov",
]
_COMPANY_BASES = [
    "Northwind", "Contoso", "Acme", "Globex", "Initech", "Umbra", "Vertex",
    "Cascade", "Ironclad", "Meridian", "Kestrel", "Solstice", "Aurora",
    "Redwood", "Sterling", "Beacon", "Cobalt", "Everest", "Harbor", "Lattice",
    "Nimbus", "Pinnacle", "Quantum", "Riverstone", "Summit", "Tideline",
]
_COMPANY_QUALIFIERS = [
    "Analytics", "Logistics", "Systems", "Labs", "Digital", "Robotics",
    "Biotech", "Capital", "Foods", "Energy", "Health", "Media", "Retail",
    "Software", "Manufacturing", "Consulting", "Freight", "Networks",
]
_COMPANY_SUFFIXES = ["Inc", "LLC", "Corp", "Group", "Holdings", "Co", "Partners", "GmbH"]
_INDUSTRIES = [
    "Agriculture", "Banking", "Biotechnology", "Chemicals", "Communications",
    "Construction", "Consulting", "Education", "Electronics", "Energy",
    "Engineering", "Entertainment", "Finance", "Healthcare", "Hospitality",
    "Insurance", "Manufacturing", "Media", "Retail", "Technology",
    "Telecommunications", "Transportation", "Utilities",
]
_RATINGS = ["Hot", "Warm", "Cold"]
_ACCOUNT_TYPES = [
    "Prospect", "Customer - Direct", "Customer - Channel",
    "Channel Partner / Reseller", "Technology Partner", "Other",
]
_LEAD_STATUSES = ["Open - Not Contacted", "Working - Contacted"]
_LEAD_SOURCES = ["Web", "Phone Inquiry", "Partner Referral", "Purchased List", "Other"]
_OPP_STAGES = [
    "Prospecting", "Qualification", "Needs Analysis", "Value Proposition",
    "Id. Decision Makers", "Proposal/Price Quote", "Negotiation/Review",
]
_OPP_STAGE_ADVANCE = [*_OPP_STAGES, "Closed Won", "Closed Lost"]
_CASE_STATUSES = ["New", "Working", "Escalated"]
_CASE_ORIGINS = ["Phone", "Email", "Web"]
_CASE_PRIORITIES = ["High", "Medium", "Low"]
_CASE_TYPES = ["Question", "Problem", "Feature Request"]
# (city, country) using full-name country integration values so they validate
# whether or not the org has State & Country picklists enabled. State is
# deliberately omitted — its valid set is country-dependent and picklist-fussy.
_CITIES = [
    ("San Francisco", "United States"), ("Austin", "United States"),
    ("New York", "United States"), ("London", "United Kingdom"),
    ("Berlin", "Germany"), ("Toronto", "Canada"),
    ("Sydney", "Australia"), ("Singapore", "Singapore"),
    ("Dublin", "Ireland"), ("Bengaluru", "India"),
]
_TITLES = [
    "VP Engineering", "Director of Operations", "Account Executive",
    "Procurement Manager", "CTO", "Head of Marketing", "IT Administrator",
    "Product Manager", "Finance Lead", "Customer Success Manager",
]
_TASK_SUBJECTS = [
    "Follow up", "Send quote", "Schedule demo", "Call back", "Email proposal",
    "Prepare contract", "Check in", "Renewal discussion", "Onboarding call",
]
_EVENT_SUBJECTS = [
    "Discovery call", "Product demo", "Quarterly review", "Kickoff meeting",
    "Contract review", "Solution workshop", "Executive briefing",
]


def _rand_company() -> str:
    base = random.choice(_COMPANY_BASES)
    if random.random() < 0.7:
        base = f"{base} {random.choice(_COMPANY_QUALIFIERS)}"
    return f"{base} {random.choice(_COMPANY_SUFFIXES)}"


def _rand_person() -> tuple[str, str]:
    return random.choice(_FIRST_NAMES), random.choice(_LAST_NAMES)


def _rand_email(first: str, last: str) -> str:
    domain = random.choice(["example.com", "mailinator.com", "test.dev", "acme-demo.co"])
    sep = random.choice([".", "_", ""])
    tag = "".join(random.choices(string.digits, k=random.randint(0, 3)))
    return f"{first.lower()}{sep}{last.lower()}{tag}@{domain}"


def _rand_phone() -> str:
    return f"+1 {random.randint(200, 989)}-{random.randint(200, 989)}-{random.randint(1000, 9999)}"


def _future_date(max_days: int = 120) -> str:
    return (datetime.now(UTC) + timedelta(days=random.randint(1, max_days))).strftime("%Y-%m-%d")


def _soon_datetime() -> str:
    dt = datetime.now(UTC) + timedelta(hours=random.randint(1, 240))
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _desc(kind: str) -> str:
    return f"{MARKER} synthetic {kind} generated for observability testing."


# ---------------------------------------------------------------------------
# Config / env
# ---------------------------------------------------------------------------


def load_env_file(path: Path) -> None:
    """Load simple KEY=VALUE lines from *path* into os.environ (no overwrite)."""
    if not path.exists():
        raise SystemExit(f"env file not found: {path}")
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def _require(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise SystemExit(f"missing required env var: {name}")
    return val


# ---------------------------------------------------------------------------
# Salesforce REST client
# ---------------------------------------------------------------------------


class Salesforce:
    """Thin async Salesforce REST wrapper with JWT-bearer auth + 401 refresh."""

    def __init__(self, client: httpx.AsyncClient, api_version: str) -> None:
        self._client = client
        self.api_version = api_version
        self.login_url = _require("SF_LOGIN_URL").rstrip("/")
        self.client_id = _require("SF_CLIENT_ID")
        self.username = _require("SF_USERNAME")
        key_path = Path(_require("SF_PRIVATE_KEY_FILE"))
        self._private_key = key_path.read_text()
        self.instance_url: str = ""
        self._token: str = ""
        self.login_count = 0

    @property
    def _base(self) -> str:
        return f"{self.instance_url}/services/data/v{self.api_version}"

    def _mint_jwt(self) -> str:
        payload = {
            "iss": self.client_id,
            "sub": self.username,
            "aud": self.login_url,
            "exp": datetime.now(UTC) + _JWT_LIFETIME,
        }
        return jwt.encode(payload, self._private_key, algorithm="RS256")

    async def login(self) -> None:
        """(Re-)authenticate. Each call produces a fresh Login event + stream."""
        resp = await self._client.post(
            f"{self.login_url}/services/oauth2/token",
            data={
                "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                "assertion": self._mint_jwt(),
            },
        )
        if not resp.is_success:
            raise SystemExit(f"auth failed HTTP {resp.status_code}: {resp.text}")
        body = resp.json()
        self._token = body["access_token"]
        self.instance_url = body["instance_url"].rstrip("/")
        self.login_count += 1
        log.info("authenticated (login #%d) -> %s", self.login_count, self.instance_url)

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}", "Content-Type": "application/json"}

    async def _request(self, method: str, url: str, **kw) -> httpx.Response:
        headers = {**self._headers(), **kw.pop("headers", {})}
        resp = await self._client.request(method, url, headers=headers, **kw)
        if resp.status_code == 401:  # token expired mid-run — re-auth once and retry
            await self.login()
            headers = {**self._headers(), **kw.pop("headers", {})}
            resp = await self._client.request(method, url, headers=headers, **kw)
        return resp

    async def create(self, sobject: str, fields: dict) -> str | None:
        resp = await self._request("POST", f"{self._base}/sobjects/{sobject}", json=fields)
        if resp.status_code == 201:
            return resp.json()["id"]
        log.warning("create %s failed HTTP %d: %s", sobject, resp.status_code, resp.text[:300])
        return None

    async def update(self, sobject: str, record_id: str, fields: dict) -> bool:
        resp = await self._request(
            "PATCH", f"{self._base}/sobjects/{sobject}/{record_id}", json=fields
        )
        if resp.status_code == 204:
            return True
        log.warning("update %s/%s failed HTTP %d", sobject, record_id, resp.status_code)
        return False

    async def query(self, soql: str) -> dict:
        resp = await self._request(
            "GET", f"{self._base}/query", params={"q": soql}
        )
        resp.raise_for_status()
        return resp.json()

    async def search(self, sosl: str) -> None:
        resp = await self._request("GET", f"{self._base}/search", params={"q": sosl})
        if not resp.is_success:
            log.debug("search failed HTTP %d: %s", resp.status_code, resp.text[:200])

    async def list_reports(self) -> list[str]:
        resp = await self._request("GET", f"{self._base}/analytics/reports")
        if not resp.is_success:
            return []
        return [r["id"] for r in resp.json()]

    async def run_report(self, report_id: str) -> None:
        resp = await self._request(
            "GET", f"{self._base}/analytics/reports/{report_id}",
            params={"includeDetails": "false"},
        )
        if not resp.is_success:
            log.debug("report %s failed HTTP %d", report_id, resp.status_code)

    async def run_apex(self, body: str) -> None:
        resp = await self._request(
            "GET", f"{self._base}/tooling/executeAnonymous/",
            params={"anonymousBody": body},
        )
        if not resp.is_success:
            log.debug("apex failed HTTP %d: %s", resp.status_code, resp.text[:200])

    async def bulk_insert(self, sobject: str, csv_body: str) -> None:
        create = await self._request(
            "POST", f"{self._base}/jobs/ingest",
            json={
                "object": sobject,
                "operation": "insert",
                "lineEnding": "LF",
                "contentType": "CSV",
            },
        )
        if create.status_code != 200:
            log.debug("bulk create job failed HTTP %d: %s", create.status_code, create.text[:200])
            return
        job_id = create.json()["id"]
        put = await self._request(
            "PUT", f"{self._base}/jobs/ingest/{job_id}/batches",
            content=csv_body, headers={"Content-Type": "text/csv"},
        )
        if put.status_code not in (200, 201):
            log.debug("bulk upload failed HTTP %d", put.status_code)
            return
        await self._request(
            "PATCH", f"{self._base}/jobs/ingest/{job_id}",
            json={"state": "UploadComplete"},
        )

    async def delete_many(self, ids: list[str]) -> int:
        """Delete up to 200 records via the sObject Collections API. Returns count deleted."""
        deleted = 0
        for i in range(0, len(ids), 200):
            chunk = ids[i : i + 200]
            resp = await self._request(
                "DELETE", f"{self._base}/composite/sobjects",
                params={"ids": ",".join(chunk), "allOrNone": "false"},
            )
            if resp.is_success:
                deleted += sum(1 for r in resp.json() if r.get("success"))
            else:
                log.warning("delete batch failed HTTP %d: %s", resp.status_code, resp.text[:200])
        return deleted


# ---------------------------------------------------------------------------
# Activity engine
# ---------------------------------------------------------------------------

# Object types that carry the Description marker (used for cleanup + relationships).
CLEANUP_OBJECTS = ["Task", "Event", "Case", "Opportunity", "Contact", "Lead", "Account"]


class ActivityEngine:
    def __init__(self, sf: Salesforce) -> None:
        self.sf = sf
        # bounded pools of recently created ids, so updates/relationships work
        self.pools: dict[str, deque[str]] = defaultdict(lambda: deque(maxlen=200))
        self.counts: dict[str, int] = defaultdict(int)
        self._reports: list[str] = []

    def _remember(self, sobject: str, record_id: str | None) -> str | None:
        if record_id:
            self.pools[sobject].append(record_id)
            self.counts[f"create:{sobject}"] += 1
        return record_id

    def _pick(self, sobject: str) -> str | None:
        pool = self.pools[sobject]
        return random.choice(pool) if pool else None

    async def _ensure_account(self) -> str | None:
        return self._pick("Account") or await self.act_create_account()

    # --- create actions -------------------------------------------------

    async def act_create_account(self) -> str | None:
        city, country = random.choice(_CITIES)
        fields = {
            "Name": _rand_company(),
            "Industry": random.choice(_INDUSTRIES),
            "Type": random.choice(_ACCOUNT_TYPES),
            "Rating": random.choice(_RATINGS),
            "AnnualRevenue": random.randint(1, 500) * 100000,
            "NumberOfEmployees": random.randint(5, 25000),
            "Phone": _rand_phone(),
            "BillingCity": city,
            "BillingCountry": country,
            "Website": "https://example.com",
            "Description": _desc("account"),
        }
        return self._remember("Account", await self.sf.create("Account", fields))

    async def act_create_contact(self) -> str | None:
        account_id = await self._ensure_account()
        first, last = _rand_person()
        fields = {
            "FirstName": first,
            "LastName": last,
            "Email": _rand_email(first, last),
            "Phone": _rand_phone(),
            "Title": random.choice(_TITLES),
            "Description": _desc("contact"),
        }
        if account_id:
            fields["AccountId"] = account_id
        return self._remember("Contact", await self.sf.create("Contact", fields))

    async def act_create_lead(self) -> str | None:
        first, last = _rand_person()
        fields = {
            "FirstName": first,
            "LastName": last,
            "Company": _rand_company(),
            "Email": _rand_email(first, last),
            "Phone": _rand_phone(),
            "Title": random.choice(_TITLES),
            "Status": random.choice(_LEAD_STATUSES),
            "LeadSource": random.choice(_LEAD_SOURCES),
            "Industry": random.choice(_INDUSTRIES),
            "Description": _desc("lead"),
        }
        return self._remember("Lead", await self.sf.create("Lead", fields))

    async def act_create_opportunity(self) -> str | None:
        account_id = await self._ensure_account()
        fields = {
            "Name": f"{_rand_company()} - {random.choice(_COMPANY_QUALIFIERS)} deal",
            "StageName": random.choice(_OPP_STAGES),
            "CloseDate": _future_date(),
            "Amount": random.randint(5, 500) * 1000,
            "LeadSource": random.choice(_LEAD_SOURCES),
            "Description": _desc("opportunity"),
        }
        if account_id:
            fields["AccountId"] = account_id
        return self._remember("Opportunity", await self.sf.create("Opportunity", fields))

    async def act_create_case(self) -> str | None:
        account_id = await self._ensure_account()
        fields = {
            "Subject": random.choice(
                ["Login issue", "Billing question", "Feature request", "Integration error",
                 "Performance degradation", "Data import help", "Access request"]
            ),
            "Status": random.choice(_CASE_STATUSES),
            "Origin": random.choice(_CASE_ORIGINS),
            "Priority": random.choice(_CASE_PRIORITIES),
            "Type": random.choice(_CASE_TYPES),
            "Description": _desc("case"),
        }
        if account_id:
            fields["AccountId"] = account_id
        contact_id = self._pick("Contact")
        if contact_id:
            fields["ContactId"] = contact_id
        return self._remember("Case", await self.sf.create("Case", fields))

    async def act_create_task(self) -> str | None:
        fields = {
            "Subject": random.choice(_TASK_SUBJECTS),
            "Status": random.choice(["Not Started", "In Progress", "Completed"]),
            "Priority": random.choice(["High", "Normal", "Low"]),
            "ActivityDate": _future_date(30),
            "Description": _desc("task"),
        }
        who = self._pick("Contact")
        what = self._pick("Opportunity") or self._pick("Account")
        if who:
            fields["WhoId"] = who
        if what:
            fields["WhatId"] = what
        return self._remember("Task", await self.sf.create("Task", fields))

    async def act_create_event(self) -> str | None:
        fields = {
            "Subject": random.choice(_EVENT_SUBJECTS),
            "ActivityDateTime": _soon_datetime(),
            "DurationInMinutes": random.choice([15, 30, 45, 60]),
            "Description": _desc("event"),
        }
        who = self._pick("Contact")
        what = self._pick("Opportunity") or self._pick("Account")
        if who:
            fields["WhoId"] = who
        if what:
            fields["WhatId"] = what
        return self._remember("Event", await self.sf.create("Event", fields))

    # --- mutate actions -------------------------------------------------

    async def act_update_record(self) -> None:
        sobject = random.choice(["Account", "Contact", "Lead", "Case"])
        rid = self._pick(sobject)
        if not rid:
            return
        patches = {
            "Account": {
                "Rating": random.choice(_RATINGS),
                "NumberOfEmployees": random.randint(5, 25000),
            },
            "Contact": {"Title": random.choice(_TITLES), "Phone": _rand_phone()},
            "Lead": {"Status": random.choice(_LEAD_STATUSES)},
            "Case": {
                "Priority": random.choice(_CASE_PRIORITIES),
                "Status": random.choice(_CASE_STATUSES),
            },
        }[sobject]
        if await self.sf.update(sobject, rid, patches):
            self.counts[f"update:{sobject}"] += 1

    async def act_advance_opportunity(self) -> None:
        rid = self._pick("Opportunity")
        if not rid:
            return
        stage = random.choice(_OPP_STAGE_ADVANCE)
        if await self.sf.update("Opportunity", rid, {"StageName": stage}):
            self.counts["advance:Opportunity"] += 1

    # NOTE: deliberately no "close case" action — closing a Case fires
    # Salesforce's post-close Survey email flow, which spams the org user's
    # mailbox. Cases are created and worked/escalated but never closed here.

    # --- read / misc actions -------------------------------------------

    async def act_query(self) -> None:
        soql = random.choice(
            [
                "SELECT Id, Name, Industry FROM Account ORDER BY CreatedDate DESC LIMIT 20",
                "SELECT Id, Name, Email FROM Contact ORDER BY CreatedDate DESC LIMIT 20",
                "SELECT Id, Name, StageName, Amount FROM Opportunity "
                "WHERE IsClosed = false LIMIT 20",
                "SELECT Id, Subject, Status FROM Case WHERE Status != 'Closed' LIMIT 20",
                "SELECT Id, Company, Status FROM Lead ORDER BY CreatedDate DESC LIMIT 20",
                "SELECT COUNT() FROM Task",
            ]
        )
        with contextlib.suppress(httpx.HTTPStatusError):
            await self.sf.query(soql)
            self.counts["query"] += 1

    async def act_search(self) -> None:
        term = random.choice(_COMPANY_BASES + _LAST_NAMES + _FIRST_NAMES)
        sosl = (
            f"FIND {{{term}}} IN ALL FIELDS RETURNING "
            "Account(Id, Name), Contact(Id, Name), Lead(Id, Name), Opportunity(Id, Name)"
        )
        await self.sf.search(sosl)
        self.counts["search"] += 1

    async def act_report(self) -> None:
        if not self._reports:
            self._reports = await self.sf.list_reports()
        if not self._reports:
            return
        await self.sf.run_report(random.choice(self._reports))
        self.counts["report"] += 1

    async def act_apex(self) -> None:
        n = random.randint(1, 5)
        body = (
            "List<Account> a = [SELECT Id, Name FROM Account "
            f"ORDER BY CreatedDate DESC LIMIT {n}]; "
            f"System.debug('sf2loki synthetic apex saw ' + a.size() + ' accounts');"
        )
        await self.sf.run_apex(urllib.parse.quote(body))
        self.counts["apex"] += 1

    async def act_bulk(self) -> None:
        rows = ["FirstName,LastName,Email,Title,Description"]
        for _ in range(random.randint(5, 25)):
            first, last = _rand_person()
            rows.append(
                f"{first},{last},{_rand_email(first, last)},"
                f"{random.choice(_TITLES)},{MARKER} bulk contact"
            )
        await self.sf.bulk_insert("Contact", "\n".join(rows))
        self.counts["bulk:Contact"] += 1

    # --- weighted menu --------------------------------------------------

    def _menu(self) -> list[tuple]:
        return [
            (self.act_create_account, 8),
            (self.act_create_contact, 10),
            (self.act_create_lead, 8),
            (self.act_create_opportunity, 6),
            (self.act_create_case, 6),
            (self.act_create_task, 8),
            (self.act_create_event, 5),
            (self.act_update_record, 10),
            (self.act_advance_opportunity, 4),
            (self.act_query, 12),
            (self.act_search, 8),
            (self.act_report, 3),
            (self.act_apex, 2),
            (self.act_bulk, 2),
        ]

    async def tick(self) -> None:
        actions, weights = zip(*self._menu(), strict=True)
        action = random.choices(actions, weights=weights, k=1)[0]
        try:
            await action()
        except Exception as exc:
            log.warning("action %s raised: %s", action.__name__, exc)

    def summary(self) -> str:
        if not self.counts:
            return "no activity yet"
        return ", ".join(f"{k}={v}" for k, v in sorted(self.counts.items()))


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


async def cleanup(sf: Salesforce) -> None:
    """Delete every record this tool ever created (Description carries MARKER)."""
    total = 0
    like = MARKER.replace("%", r"\%").replace("_", r"\_")
    for sobject in CLEANUP_OBJECTS:
        ids: list[str] = []
        soql = f"SELECT Id FROM {sobject} WHERE Description LIKE '%{like}%'"
        try:
            result = await sf.query(soql)
        except httpx.HTTPStatusError as exc:
            log.warning("cleanup query %s failed: %s", sobject, exc)
            continue
        ids.extend(r["Id"] for r in result.get("records", []))
        while not result.get("done", True) and result.get("nextRecordsUrl"):
            resp = await sf._request("GET", f"{sf.instance_url}{result['nextRecordsUrl']}")
            resp.raise_for_status()
            result = resp.json()
            ids.extend(r["Id"] for r in result.get("records", []))
        if ids:
            deleted = await sf.delete_many(ids)
            total += deleted
            log.info("deleted %d %s record(s)", deleted, sobject)
    log.info("cleanup complete — %d record(s) removed", total)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


async def run(args: argparse.Namespace) -> None:
    async with httpx.AsyncClient(timeout=30.0) as client:
        sf = Salesforce(client, args.api_version)
        await sf.login()

        if args.cleanup:
            await cleanup(sf)
            return

        engine = ActivityEngine(sf)
        stop = asyncio.Event()

        def _handle_signal() -> None:
            log.info("shutdown requested — finishing up")
            stop.set()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, _handle_signal)

        interval = 60.0 / max(args.ops_per_min, 1)
        deadline = (
            loop.time() + args.duration if args.duration > 0 else None
        )
        last_login = loop.time()
        ticks = 0
        log.info(
            "generating activity: ~%d ops/min (every ~%.1fs), relogin every %ds%s",
            args.ops_per_min, interval, args.relogin_every,
            f", for {args.duration}s" if args.duration else " (Ctrl-C to stop)",
        )

        while not stop.is_set():
            await engine.tick()
            ticks += 1
            if ticks % 20 == 0:
                log.info("[%d ops] %s", ticks, engine.summary())

            now = loop.time()
            if now - last_login >= args.relogin_every:
                await sf.login()
                last_login = now
            if deadline is not None and now >= deadline:
                log.info("duration reached")
                break

            # jittered pacing so the traffic pattern looks organic
            delay = random.uniform(interval * 0.4, interval * 1.6)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=delay)

        log.info("done after %d ops — %s", ticks, engine.summary())
        log.info("logins performed: %d", sf.login_count)
        log.info("run with --cleanup to delete the %s-tagged records", MARKER)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--env-file", type=Path, help="load KEY=VALUE env vars from this file (e.g. .env.dev)"
    )
    p.add_argument(
        "--ops-per-min", type=int, default=6, help="approx operations per minute (default 6)"
    )
    p.add_argument(
        "--duration", type=int, default=0, help="run for N seconds then stop (0 = until Ctrl-C)"
    )
    p.add_argument(
        "--relogin-every", type=int, default=420,
        help="re-authenticate every N seconds (Login events)",
    )
    p.add_argument(
        "--api-version", default=os.environ.get("SF_API_VERSION", "60.0"),
        help="Salesforce API version",
    )
    p.add_argument(
        "--cleanup", action="store_true", help="delete all records this tool created, then exit"
    )
    p.add_argument("--verbose", "-v", action="store_true", help="debug logging")
    return p.parse_args(argv)


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
        stream=sys.stderr,
    )
    if not args.verbose:
        # httpx logs every request at INFO; keep our own summary lines readable.
        logging.getLogger("httpx").setLevel(logging.WARNING)
    if args.env_file:
        load_env_file(args.env_file)
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
