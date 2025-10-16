"""Scraper implementation for elcats.ru with real navigation flow."""

from __future__ import annotations

import html
import logging
import re
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag
from requests import Response
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .storage import Storage

LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass
class Brand:
    slug: str
    name: str


@dataclass
class Model:
    slug: str
    label: str


@dataclass
class Modification:
    code: str
    title: str


@dataclass
class Vehicle:
    id: str
    name: str
    model_slug: str
    model_label: str
    modification_code: str
    modification_title: str
    options: Dict[str, str]


@dataclass
class Group:
    id: str
    name: str
    title: str


@dataclass
class UnitNode:
    id: str
    name: str


@dataclass
class Part:
    key: str
    code: Optional[str]
    name: str
    quantity: Optional[str]
    period: Optional[str]
    info: Optional[str]
    price_text: Optional[str]
    pc_id: Optional[str]


@dataclass
class CallbackContext:
    url: str
    viewstate: str
    viewstategen: str
    eventvalidation: str


BRAND_LINK_RE = re.compile(r"^/[\w-]+/?$")
MODEL_LINK_RE = re.compile(r"javascript:submit\('([^']+)'\);?")
MODIFICATION_LINK_RE = re.compile(r"javascript:submit\('([^']+)'\s*,\s*'([^']*)'\);?")
GROUP_LINK_RE = re.compile(
    r"javascript:submit\('([^']+)'\s*,\s*'([^']+)'\s*,\s*'([^']*)'\s*,\s*'([^']*)'\);?"
)
PRICE_LINK_RE = re.compile(r"javascript:submit\('([^']+)'\s*,")


class ElcatsScraper:
    """High level scraper that populates a :class:`Storage` instance."""

    def __init__(
        self,
        storage: Storage,
        base_url: str = "https://www.elcats.ru",
        delay: float = 0.2,
        session: Optional[requests.Session] = None,
        max_workers: int = 1,
    ) -> None:
        self.storage = storage
        self.base_url = base_url.rstrip("/") + "/"
        self.delay = max(delay, 0.0)
        self._base_session = session
        self._thread_local = threading.local()
        self.max_workers = max(1, int(max_workers))

    # ------------------------------------------------------------------
    # Session helpers
    # ------------------------------------------------------------------

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST"),
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (compatible; ElcatsScraper/1.1; +https://www.example.com/bot)",
                "Accept-Language": "ru,en;q=0.8",
            }
        )
        return session

    def _clone_session(self, base: requests.Session) -> requests.Session:
        session = requests.Session()
        session.headers.update(base.headers)
        session.cookies.update(base.cookies)
        session.auth = base.auth
        session.proxies = base.proxies
        session.verify = base.verify
        session.cert = base.cert
        session.hooks = base.hooks
        session.trust_env = base.trust_env
        for prefix, adapter in base.adapters.items():
            session.mount(prefix, adapter)
        return session

    def _get_session(self) -> requests.Session:
        session = getattr(self._thread_local, "session", None)
        if session is None:
            if self._base_session is not None:
                session = self._clone_session(self._base_session)
            else:
                session = self._build_session()
            self._thread_local.session = session
        return session

    def _request(self, method: str, path: str, **kwargs: object) -> Response:
        url = path if path.startswith("http") else urljoin(self.base_url, path)
        LOGGER.debug("%s %s params=%s", method, url, kwargs.get("params"))
        session = self._get_session()
        response = session.request(method, url, timeout=60, **kwargs)
        response.raise_for_status()
        response.encoding = "utf-8"
        time.sleep(self.delay)
        return response

    def _get_soup(self, path: str, params: Optional[dict[str, str]] = None) -> tuple[BeautifulSoup, Response]:
        response = self._request("GET", path, params=params)
        return BeautifulSoup(response.text, "html.parser"), response

    def _post_callback(self, context: CallbackContext, payload: str) -> str:
        response = self._request(
            "POST",
            context.url,
            data={
                "__EVENTTARGET": "",
                "__EVENTARGUMENT": "",
                "__VIEWSTATE": context.viewstate,
                "__VIEWSTATEGENERATOR": context.viewstategen,
                "__EVENTVALIDATION": context.eventvalidation,
                "__CALLBACKID": "__Page",
                "__CALLBACKPARAM": payload,
            },
        )
        text = response.text
        if "|" in text:
            text = text.split("|", 1)[1]
        return text

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scrape(self, brand_slugs: Optional[Iterable[str]] = None) -> None:
        brand_filter = {slug.strip().lower() for slug in brand_slugs or []} or None
        for brand in self._iter_brands():
            if brand_filter and brand.slug not in brand_filter:
                continue
            LOGGER.info("Processing brand %s", brand.name)
            brand_id = self.storage.add_brand(brand.slug, brand.name)
            try:
                self._scrape_brand(brand, brand_id)
            except Exception:  # pragma: no cover - safety guard
                LOGGER.exception("Failed to scrape brand %s", brand.slug)
            self.storage.commit()

    # ------------------------------------------------------------------
    # Brand/model/modification traversal
    # ------------------------------------------------------------------

    def _scrape_brand(self, brand: Brand, brand_id: str) -> None:
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures: List[Future[None]] = []
            for model in self._iter_models(brand):
                LOGGER.info("  Model %s", model.label)
                for modification in self._iter_modifications(brand.slug, model):
                    futures.append(
                        executor.submit(
                            self._process_vehicle_task,
                            brand.slug,
                            brand_id,
                            model,
                            modification,
                        )
                    )
            for future in futures:
                try:
                    future.result()
                except Exception:
                    LOGGER.exception(
                        "Unexpected failure while processing brand %s", brand.slug
                    )

    def _process_vehicle_task(
        self, brand_slug: str, brand_id: str, model: Model, modification: Modification
    ) -> None:
        LOGGER.info("    Modification %s", modification.title or modification.code)
        if not self.storage.should_process_modification(
            brand_slug, model.slug, modification.code
        ):
            LOGGER.info(
                "    Skipping %s/%s/%s — already completed",
                brand_slug,
                model.slug,
                modification.code,
            )
            return
        try:
            vehicle = self._build_vehicle(brand_slug, model, modification)
        except Exception:
            LOGGER.exception(
                "Failed to resolve vehicle for model %s modification %s",
                model.slug,
                modification.code,
            )
            return
        vehicle_id = self.storage.add_vehicle(vehicle.id, vehicle.name, brand_id)
        self.storage.add_vehicle_attribute(vehicle_id, "model_slug", vehicle.model_slug)
        self.storage.add_vehicle_attribute(vehicle_id, "model_label", vehicle.model_label)
        self.storage.add_vehicle_attribute(
            vehicle_id, "modification_code", vehicle.modification_code
        )
        self.storage.add_vehicle_attribute(
            vehicle_id, "modification_title", vehicle.modification_title
        )
        for option_key, option_value in vehicle.options.items():
            self.storage.add_vehicle_attribute(
                vehicle_id, f"option:{option_key}", option_value
            )
        try:
            self._scrape_vehicle(brand_slug, vehicle, vehicle_id)
        except Exception:
            LOGGER.exception(
                "Failed to scrape vehicle data for %s (%s)",
                vehicle.modification_title,
                vehicle.id,
            )
        else:
            self.storage.mark_modification_completed(
                brand_slug, model.slug, modification.code
            )

    def _build_vehicle(self, brand_slug: str, model: Model, modification: Modification) -> Vehicle:
        model_id, options = self._resolve_model_identifier(brand_slug, modification)
        label = modification.title.strip() if modification.title else modification.code
        name = f"{model.label} — {label}" if model.label else label
        return Vehicle(
            id=model_id,
            name=name,
            model_slug=model.slug,
            model_label=model.label,
            modification_code=modification.code,
            modification_title=modification.title,
            options=options,
        )

    # ------------------------------------------------------------------
    # Page iterators
    # ------------------------------------------------------------------

    def _iter_brands(self) -> Iterator[Brand]:
        soup, _ = self._get_soup("/")
        seen: set[str] = set()
        for anchor in soup.select("a"):
            href = anchor.get("href", "")
            if not href or not BRAND_LINK_RE.match(href):
                continue
            slug = href.strip("/").lower()
            if not slug or slug in seen:
                continue
            name = anchor.get_text(strip=True)
            if not name:
                continue
            seen.add(slug)
            yield Brand(slug=slug, name=name)

    def _iter_models(self, brand: Brand) -> Iterator[Model]:
        soup, _ = self._get_soup(f"{brand.slug}/")
        models: dict[str, str] = {}
        for anchor in soup.select("a[href^='javascript:submit(']"):
            href = anchor.get("href", "")
            match = MODEL_LINK_RE.match(href or "")
            if not match:
                continue
            slug = match.group(1).strip()
            if not slug:
                continue
            label = anchor.get_text(" ", strip=True)
            if not label:
                label = slug
            # Prefer longer label descriptions
            previous = models.get(slug)
            if not previous or len(label) > len(previous):
                models[slug] = label
        for slug, label in models.items():
            yield Model(slug=slug, label=label)

    def _iter_modifications(self, brand_slug: str, model: Model) -> Iterator[Modification]:
        soup, _ = self._get_soup(f"{brand_slug}/Modification.aspx", params={"Model": model.slug})
        seen: set[str] = set()
        for anchor in soup.select("a[href^='javascript:submit(']"):
            href = anchor.get("href", "")
            match = MODIFICATION_LINK_RE.match(href or "")
            if not match:
                continue
            code = match.group(1).strip()
            title = html.unescape(match.group(2).strip())
            if not code or code in seen:
                continue
            seen.add(code)
            yield Modification(code=code, title=title or code)

    def _resolve_model_identifier(
        self, brand_slug: str, modification: Modification
    ) -> tuple[str, Dict[str, str]]:
        params = {"Code": modification.code, "Title": modification.title}
        soup, response = self._get_soup(f"{brand_slug}/Options.aspx", params=params)
        viewstate = self._require_input(soup, "__VIEWSTATE")
        viewstategen = self._require_input(soup, "__VIEWSTATEGENERATOR")
        eventvalidation = self._require_input(soup, "__EVENTVALIDATION")
        options: Dict[str, str] = {}
        for radio in soup.select(".hyundai-options input[type=radio]"):
            name = radio.get("name")
            value = radio.get("value")
            if not name or value is None:
                continue
            if radio.has_attr("checked") or name not in options:
                options[name] = value
        option_payload = ";".join(f"{key},{value}" for key, value in sorted(options.items()))
        if option_payload:
            callback_param = f"1;{option_payload};"
        else:
            callback_param = "1;"
        context = CallbackContext(
            url=response.url,
            viewstate=viewstate,
            viewstategen=viewstategen,
            eventvalidation=eventvalidation,
        )
        payload = self._post_callback(context, callback_param)
        parts = payload.split("^")
        if len(parts) < 2:
            raise RuntimeError(f"Unexpected options callback payload: {payload}")
        status, identifier = parts[0], parts[1]
        if status != "1" or not identifier:
            raise RuntimeError(f"Failed to resolve model identifier: {payload}")
        return identifier, options

    def _iter_groups(self, brand_slug: str, vehicle: Vehicle) -> Iterator[Group]:
        soup, _ = self._get_soup(
            f"{brand_slug}/Group.aspx",
            params={"Model": vehicle.id},
        )
        seen: set[str] = set()
        for anchor in soup.select("a[href^='javascript:submit(']"):
            href = anchor.get("href", "")
            match = GROUP_LINK_RE.match(href or "")
            if not match:
                continue
            group_id = match.group(1).strip()
            title = html.unescape(match.group(3).strip())
            if not group_id or group_id in seen:
                continue
            seen.add(group_id)
            text = anchor.get_text(" ", strip=True)
            name = text
            # Normalise display like "[ 81-810 ] NAME"
            if "]" in text:
                name = text.split("]", 1)[-1].strip()
            yield Group(id=group_id, name=name, title=title or name)

    def _fetch_unit_page(
        self, brand_slug: str, vehicle: Vehicle, group: Group
    ) -> tuple[CallbackContext, List[UnitNode]]:
        params = {"GroupId": group.id, "Model": vehicle.id, "Title": group.title}
        soup, response = self._get_soup(f"{brand_slug}/Unit.aspx", params=params)
        viewstate = self._require_input(soup, "__VIEWSTATE")
        viewstategen = self._require_input(soup, "__VIEWSTATEGENERATOR")
        eventvalidation = self._require_input(soup, "__EVENTVALIDATION")
        nodes: List[UnitNode] = []
        seen: set[str] = set()
        for div in soup.select("div.CNode"):
            node_id = div.get("id")
            if not node_id or node_id in seen:
                continue
            seen.add(node_id)
            name = div.get_text(" ", strip=True)
            nodes.append(UnitNode(id=node_id, name=name))
        context = CallbackContext(
            url=response.url,
            viewstate=viewstate,
            viewstategen=viewstategen,
            eventvalidation=eventvalidation,
        )
        return context, nodes

    def _iter_unit_parts(self, context: CallbackContext, node_id: str) -> Iterator[Part]:
        payload = self._post_callback(context, node_id)
        if not payload.strip():
            return
        soup = BeautifulSoup(payload, "html.parser")
        table = soup.find("table")
        if not table:
            return
        rows = table.find_all("tr")
        if not rows:
            return
        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 5:
                continue
            code_cell = cells[0]
            code_text = code_cell.get_text(strip=True) or None
            name = cells[1].get_text(" ", strip=True)
            quantity = cells[2].get_text(strip=True) or None
            period = cells[3].get_text(" ", strip=True) or None
            info = cells[4].get_text(" ", strip=True) or None
            price_text = cells[5].get_text(" ", strip=True) if len(cells) > 5 else None
            pc_id = self._extract_pc_id(code_cell)
            if not name and not code_text and not pc_id:
                continue
            key = pc_id or code_text or f"node:{node_id}:{name}"
            yield Part(
                key=key,
                code=code_text,
                name=name,
                quantity=quantity,
                period=period,
                info=info,
                price_text=price_text,
                pc_id=pc_id,
            )

    # ------------------------------------------------------------------
    # Vehicle scraping
    # ------------------------------------------------------------------

    def _scrape_vehicle(self, brand_slug: str, vehicle: Vehicle, vehicle_id: str) -> None:
        for group in self._iter_groups(brand_slug, vehicle):
            LOGGER.info("      Group %s", group.name)
            category_id = self.storage.add_category(
                vehicle.id,
                f"group:{group.id}",
                group.name,
                code=group.id,
            )
            context, nodes = self._fetch_unit_page(brand_slug, vehicle, group)
            for node in nodes:
                LOGGER.info("        Unit %s", node.name)
                subgroup_id = self.storage.add_category(
                    vehicle.id,
                    f"unit:{group.id}:{node.id}",
                    node.name,
                    parent_id=category_id,
                )
                for part in self._iter_unit_parts(context, node.id):
                    part_id = self.storage.add_part(
                        vehicle.id,
                        node.id,
                        part.key,
                        part.name,
                        part.code,
                        subgroup_id,
                    )
                    self.storage.link_part_vehicle(part_id, vehicle_id)
                    self.storage.add_part_attribute(part_id, "quantity", part.quantity)
                    self.storage.add_part_attribute(part_id, "period", part.period)
                    self.storage.add_part_attribute(part_id, "info", part.info)
                    self.storage.add_part_attribute(part_id, "price", part.price_text)
                    if part.pc_id:
                        self.storage.add_part_attribute(part_id, "pc_id", part.pc_id)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _require_input(self, soup: BeautifulSoup, name: str) -> str:
        element = soup.find("input", {"name": name})
        if not element or not element.has_attr("value"):
            raise RuntimeError(f"Expected input {name} not found")
        return element["value"]

    def _extract_pc_id(self, cell: Tag) -> Optional[str]:
        link = cell.find("a", href=PRICE_LINK_RE)
        if not link:
            return None
        match = PRICE_LINK_RE.search(link.get("href", ""))
        if not match:
            return None
        return match.group(1)
