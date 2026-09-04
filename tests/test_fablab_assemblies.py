"""Tests for the 865FabLab assembly flow (fablab_assemblies.py) —
pure helpers, reply parsing, DB helpers, and complete_assembly against a
mocked CIN7 (partial receipt → pick lines + remainder task)."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import db  # noqa: E402
import fablab_assemblies as fa  # noqa: E402


class _TempDb(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._p = patch.object(
            db, "DB_PATH", str(Path(self._tmpdir.name) / "t.db"))
        self._p.start()
        with db.connect():
            pass

    def tearDown(self) -> None:
        self._p.stop()
        self._tmpdir.cleanup()


BOM = {
    "LED-UNI-TILE12-180-FLAT270": [
        {"ComponentSKU": "OSC-865FABLAB-LABOR", "Quantity": 1},
        {"ComponentSKU": "LED-G2000820-0609", "Quantity": 0.5},
        {"ComponentSKU": "LED-76650038-0609", "Quantity": 0.333},
    ],
}


class PickListTests(unittest.TestCase):
    def test_excludes_labor_and_totals(self) -> None:
        per_sku, totals = fa.build_pick_list(
            {"LED-UNI-TILE12-180-FLAT270": 40}, BOM, {})
        comps = {c for c, _n, _t in per_sku[0]["components"]}
        self.assertNotIn("OSC-865FABLAB-LABOR", comps)
        self.assertAlmostEqual(totals["LED-G2000820-0609"], 20.0)
        txt = fa.format_pick_list(per_sku, totals,
                                  {"LED-UNI-TILE12-180-FLAT270": "FG-1"})
        self.assertIn("[FG-1] LED-UNI-TILE12-180-FLAT270 x 40", txt)
        self.assertIn("TOTALS TO PICK", txt)


class ParseReplyTests(unittest.TestCase):
    def test_done_variants(self) -> None:
        self.assertEqual(fa.parse_reply("done"), {"qty": None, "overrides": {}})
        self.assertEqual(fa.parse_reply("Done 35")["qty"], 35.0)
        self.assertEqual(fa.parse_reply("received 12")["qty"], 12.0)
        self.assertIsNone(fa.parse_reply("how is this going?"))
        self.assertIsNone(fa.parse_reply(""))

    def test_overrides(self) -> None:
        r = fa.parse_reply("done 35\nLED-G2000820-0609 = 0\nled-76650038-0609: 10")
        self.assertEqual(r["overrides"],
                         {"LED-G2000820-0609": 0.0, "LED-76650038-0609": 10.0})


class DbHelperTests(_TempDb):
    def test_assembly_lifecycle_and_settings(self) -> None:
        did = db.create_po_draft(supplier="865FabLab", name="t", actor="u")
        aid = db.create_fablab_assembly(
            did, "SKU-A", 40, status="authorised", cin7_task_id="T1",
            assembly_number="FG-1", response={}, actor="u")
        self.assertEqual(len(db.list_fablab_assemblies(did)), 1)
        db.set_fablab_assembly_slack(aid, "C1", "1.0")
        self.assertEqual(db.get_fablab_assembly_by_slack("C1", "1.0")["id"], aid)
        db.mark_fablab_assembly_completed(aid, 40, "u")
        self.assertEqual(db.get_fablab_assembly(aid)["status"], "completed")
        db.record_fablab_pick_variance(aid, did, "SKU-A", 40,
                                       [("C1", 20, 0), ("C2", 13.3, 13.3)], "u")
        summ = db.fablab_pick_variance_summary()
        self.assertEqual(len(summ), 2)
        db.fablab_setting_set("labor_unit_price", "10", "u")
        self.assertEqual(db.fablab_setting_get("labor_unit_price"), "10")
        db.fablab_po_notification_upsert(did, cin7_po_number="PO-1")
        db.fablab_po_notification_upsert(did, odoo_lead_id=5)
        n = db.fablab_po_notification_get(did)
        self.assertEqual((n["cin7_po_number"], n["odoo_lead_id"]), ("PO-1", 5))


class _FakeResp:
    def __init__(self, status_code, body):
        self.status_code = status_code
        self._body = body
        self.text = str(body)

    def json(self):
        return self._body


TASK = {
    "TaskID": "T1", "AssemblyNumber": "FG-1", "Status": "AUTHORISED",
    "ProductID": "P1", "ProductName": "Corner", "Quantity": 40,
    "Location": "Main Warehouse", "WIPAccount": "120", "Account": "115",
    "WIPDate": "2026-09-04T10:00:00", "Notes": "",
    "OrderLines": [
        {"ProductID": "L", "ProductCode": "OSC-865FABLAB-LABOR",
         "Name": "Labor", "Quantity": 40, "TotalQuantity": 40},
        {"ProductID": "C1", "ProductCode": "LED-G2000820-0609",
         "Name": "Extrusion", "Quantity": 20, "TotalQuantity": 20},
        {"ProductID": "C2", "ProductCode": "LED-76650038-0609",
         "Name": "Diffuser", "Quantity": 13.32, "TotalQuantity": 13.32},
    ],
}


class CompleteAssemblyTests(_TempDb):
    def setUp(self) -> None:
        super().setUp()
        self.did = db.create_po_draft(supplier="865FabLab", name="t", actor="u")
        self.aid = db.create_fablab_assembly(
            self.did, "LED-UNI-TILE12-180-FLAT270", 40, status="authorised",
            cin7_task_id="T1", assembly_number="FG-1", response={}, actor="u")
        self.calls = []

        def fake_http(method, url, headers, json_body=None, params=None,
                      log=None, rate_s=0, last_call=0.0):
            self.calls.append((method, url.rsplit("/v2", 1)[1], json_body))
            if method == "GET" and url.endswith("/finishedGoods"):
                return _FakeResp(200, TASK), 0.0
            if method == "PUT":
                return _FakeResp(200, {**TASK, "Quantity": json_body["Quantity"]}), 0.0
            if url.endswith("/finishedGoods/pick"):
                return _FakeResp(200, {"TaskID": "T1", "Status": "COMPLETED",
                                       "Errors": []}), 0.0
            if method == "POST" and url.endswith("/finishedGoods"):
                return _FakeResp(200, {"TaskID": "T2", "AssemblyNumber": "FG-2",
                                       "Errors": []}), 0.0
            raise AssertionError(f"unexpected {method} {url}")

        self._patches = [
            patch.object(fa, "_http", side_effect=fake_http),
            patch.object(fa, "_headers", return_value={"x": "y"}),
        ]
        for p in self._patches:
            p.start()

    def tearDown(self) -> None:
        for p in self._patches:
            p.stop()
        super().tearDown()

    def test_partial_with_override_creates_remainder(self) -> None:
        r = fa.complete_assembly(
            self.aid, qty_received=30,
            pick_overrides={"LED-G2000820-0609": 0}, actor="u", apply=True)
        self.assertTrue(r["ok"], r)
        methods = [(m, u) for m, u, _b in self.calls]
        self.assertIn(("PUT", "/finishedGoods"), methods)
        pick_body = next(b for m, u, b in self.calls if u == "/finishedGoods/pick")
        codes = {p["ProductCode"]: p["Quantity"] for p in pick_body["PickLines"]}
        self.assertNotIn("LED-G2000820-0609", codes)        # override → 0, omitted
        self.assertAlmostEqual(codes["LED-76650038-0609"], 9.99, places=2)
        self.assertAlmostEqual(codes["OSC-865FABLAB-LABOR"], 30)
        self.assertEqual(r["remainder"]["qty"], 10)
        rows = db.list_fablab_assemblies(self.did)
        self.assertEqual([x["status"] for x in rows], ["completed", "authorised"])
        self.assertEqual(rows[1]["parent_task_id"], "T1")
        var = db.fablab_pick_variance_summary()
        ext = next(v for v in var if v["component_sku"] == "LED-G2000820-0609")
        self.assertEqual(ext["actual_total"], 0)
        self.assertAlmostEqual(ext["bom_total"], 15.0)

    def test_full_receipt_no_put_no_remainder(self) -> None:
        r = fa.complete_assembly(self.aid, actor="u", apply=True)
        self.assertTrue(r["ok"], r)
        self.assertNotIn("PUT", [m for m, _u, _b in self.calls])
        self.assertIsNone(r["remainder"])
        self.assertEqual(db.get_fablab_assembly(self.aid)["completed_qty"], 40)

    def test_rejects_bad_qty_and_dry_run(self) -> None:
        r = fa.complete_assembly(self.aid, qty_received=41, actor="u", apply=True)
        self.assertFalse(r["ok"])
        r = fa.complete_assembly(self.aid, qty_received=5, actor="u", apply=False)
        self.assertTrue(r["ok"])
        self.assertEqual(r["remainder"], 35)
        self.assertEqual(db.get_fablab_assembly(self.aid)["status"], "authorised")


if __name__ == "__main__":
    unittest.main()


class ComponentNotesTest(unittest.TestCase):
    def test_short_auto_assembly_points_to_master(self):
        import fablab_assemblies as fa
        totals = {"LED-G2000820-0609": 9.496, "LED-76650038-0609": 7.659}
        bom_parents = {"LED-G2000820-0609": [
            {"ComponentSKU": "LED-G2000620-2", "Quantity": 0.333}]}
        product_map = {
            "LED-G2000820-0609": {"StockLocator": "F35", "AutoAssembly": "True"},
            "LED-G2000620-2": {"StockLocator": "R1"},
            "LED-76650038-0609": {"StockLocator": "C7"}}
        stock_map = {"LED-G2000820-0609": {"OnHand": 4},
                     "LED-76650038-0609": {"OnHand": 50}}
        notes = fa.component_notes(totals, bom_parents, product_map, stock_map)
        self.assertIn("loc F35", notes["LED-G2000820-0609"])
        self.assertIn("cut 6 from master LED-G2000620-2", notes["LED-G2000820-0609"])
        self.assertIn("2 length(s)", notes["LED-G2000820-0609"])
        self.assertIn("loc R1", notes["LED-G2000820-0609"])
        self.assertNotIn("SHORT", notes["LED-76650038-0609"])
        txt = fa.format_pick_list([], totals, notes=notes)
        self.assertIn("LED-G2000820-0609 x 10  (BOM 9.496)  [loc F35", txt)
