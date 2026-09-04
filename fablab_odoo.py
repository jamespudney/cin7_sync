"""fablab_odoo.py — minimal Odoo (865FabLab) XML-RPC client for the
865FabLab toll-manufacturing flow (see fablab_assemblies.py).

When an 865FabLab labor PO is authorised in CIN7 we create, in
865FabLab's Odoo:
  1. a CRM lead (`crm.lead`) named after the PO, customer Wired4Signs
     USA, description = the end-product list + pick list, and
  2. a quotation (`sale.order`, state draft) linked to that lead with
     one line: the per-unit labor service product × total units.

Configuration (all env; the feature is silently skipped when the API
key is missing, so the CIN7/Slack side of the flow never depends on it):
  ODOO_URL                default https://www.865fablab.com
  ODOO_DB                 default 865fablab
  ODOO_USER               login (email) the API key belongs to
  ODOO_API_KEY            Odoo API key (Settings → Account Security)
  ODOO_LABOR_PRODUCT_CODE default OSC-865FABLAB-LABOR (created on first
                          use as a service product if missing)
  ODOO_LABOR_UNIT_PRICE   default 10.00 (only used when creating the
                          product; existing list_price is kept)
  ODOO_CUSTOMER_NAME      default "Wired4Signs USA"

Odoo 19 (Odoo Online), 2026-09-04.
"""

from __future__ import annotations

import logging
import os
import xmlrpc.client
from typing import Optional

log = logging.getLogger("fablab_odoo")

DEFAULT_URL = "https://www.865fablab.com"
DEFAULT_DB = "865fablab"
DEFAULT_LABOR_CODE = "OSC-865FABLAB-LABOR"
DEFAULT_LABOR_NAME = "865FabLab - Corner Assembly Labor (per unit)"
DEFAULT_CUSTOMER = "Wired4Signs USA"


def is_configured() -> bool:
    return bool(os.environ.get("ODOO_API_KEY", "").strip()
                and os.environ.get("ODOO_USER", "").strip())


class OdooClient:
    def __init__(self) -> None:
        self.url = os.environ.get("ODOO_URL", DEFAULT_URL).rstrip("/")
        self.db = os.environ.get("ODOO_DB", DEFAULT_DB)
        self.user = os.environ.get("ODOO_USER", "").strip()
        self.key = os.environ.get("ODOO_API_KEY", "").strip()
        if not self.user or not self.key:
            raise RuntimeError("ODOO_USER / ODOO_API_KEY not set")
        common = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/common")
        self.uid = common.authenticate(self.db, self.user, self.key, {})
        if not self.uid:
            raise RuntimeError("Odoo authentication failed")
        self._models = xmlrpc.client.ServerProxy(
            f"{self.url}/xmlrpc/2/object", allow_none=True)

    def call(self, model: str, method: str, *args, **kwargs):
        return self._models.execute_kw(
            self.db, self.uid, self.key, model, method, list(args), kwargs)

    def search_read(self, model, domain, fields, limit=20):
        return self.call(model, "search_read", domain,
                         fields=fields, limit=limit)

    def create(self, model, vals) -> int:
        res = self.call(model, "create", [vals])
        # Odoo 17+ returns a list of ids for create(); older returns an int.
        if isinstance(res, list):
            res = res[0]
        return int(res)

    # -- lookups ---------------------------------------------------------

    def customer_id(self) -> int:
        name = os.environ.get("ODOO_CUSTOMER_NAME", DEFAULT_CUSTOMER)
        rows = self.search_read(
            "res.partner", [["name", "=", name], ["is_company", "=", True]],
            ["id"], limit=1)
        if not rows:
            rows = self.search_read(
                "res.partner", [["name", "ilike", name]], ["id"], limit=1)
        if not rows:
            raise RuntimeError(f"Odoo customer {name!r} not found")
        return int(rows[0]["id"])

    def labor_product_id(self) -> int:
        code = os.environ.get("ODOO_LABOR_PRODUCT_CODE", DEFAULT_LABOR_CODE)
        rows = self.search_read(
            "product.product", [["default_code", "=", code]], ["id"], limit=1)
        if rows:
            return int(rows[0]["id"])
        price = float(os.environ.get("ODOO_LABOR_UNIT_PRICE", "10") or 10)
        tmpl_id = self.create("product.template", {
            "name": DEFAULT_LABOR_NAME,
            "default_code": code,
            "type": "service",
            "sale_ok": True,
            "purchase_ok": False,
            "list_price": price,
        })
        rows = self.search_read(
            "product.product", [["product_tmpl_id", "=", tmpl_id]],
            ["id"], limit=1)
        log.info("Created Odoo labor product %s (template %s)", code, tmpl_id)
        return int(rows[0]["id"])

    # -- the flow --------------------------------------------------------

    def _salesperson(self) -> dict:
        """{'user_id': id} for ODOO_SALESPERSON (default Luke Fletcher);
        empty dict if not found so creation never fails on this."""
        login = os.environ.get("ODOO_SALESPERSON", "luke@865fablab.com")
        try:
            rows = self.search_read("res.users", [["login", "=", login]],
                                    ["id"], limit=1)
            return {"user_id": rows[0]["id"]} if rows else {}
        except Exception:  # noqa: BLE001
            log.warning("Odoo salesperson %s not found", login)
            return {}

    def create_lead_and_quote(
            self, *, po_number: str, total_qty: float,
            description_html: str, unit_price: Optional[float] = None,
    ) -> dict:
        """Returns {'lead_id', 'quote_id', 'quote_name'}."""
        partner_id = self.customer_id()
        product_id = self.labor_product_id()
        lead_name = f"{po_number} — Wired4Signs corner assembly"
        lead_id = self.create("crm.lead", {
            "name": lead_name,
            "type": "opportunity",
            "partner_id": partner_id,
            **self._salesperson(),
            "description": description_html,
        })
        line = {"product_id": product_id, "product_uom_qty": float(total_qty),
                "name": f"Corner assembly labor — {po_number}"}
        if unit_price is not None:
            line["price_unit"] = float(unit_price)
        quote_id = self.create("sale.order", {
            "partner_id": partner_id,
            **self._salesperson(),
            "opportunity_id": lead_id,
            "origin": po_number,
            "client_order_ref": po_number,
            "note": description_html,
            "order_line": [[0, 0, line]],
        })
        rows = self.search_read("sale.order", [["id", "=", quote_id]],
                                ["name"], limit=1)
        quote_name = rows[0]["name"] if rows else str(quote_id)
        log.info("Odoo lead %s + quote %s created for %s",
                 lead_id, quote_name, po_number)
        return {"lead_id": lead_id, "quote_id": quote_id,
                "quote_name": quote_name}
