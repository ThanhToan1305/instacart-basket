import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / "scripts"))
from dashboard_bundle import load_bundle
from dashboard_fixture import make_fixture
from streamlit.testing.v1 import AppTest


class DashboardTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.pointer, _, _ = make_fixture(self.temp.name)

    def app(self):
        return AppTest.from_file(str(PROJECT / "app/main.py"), default_timeout=30).run()

    def test_all_five_pages_render(self):
        with patch.dict(os.environ, {"DASHBOARD_BUNDLE": str(self.pointer)}):
            app = self.app()
            self.assertFalse(app.exception)
            for page in ["Hành vi mua sắm", "Luật kết hợp", "Thử giỏ hàng", "Đánh giá & tái lập", "Tổng quan"]:
                app.radio(key="page").set_value(page).run()
                self.assertFalse(app.exception, str(app.exception))

    def test_demo_uses_shared_engine(self):
        with patch.dict(os.environ, {"DASHBOARD_BUNDLE": str(self.pointer)}):
            app = self.app()
            app.radio(key="page").set_value("Thử giỏ hàng").run()
            app.multiselect(key="cart").set_value([1]).run()
            app.button(key="recommend").click().run()
            self.assertFalse(app.exception, str(app.exception))
            result = app.session_state["demo_result"]
            self.assertEqual(result["predictions"]["hybrid"][0]["product_id"], 2)
            self.assertTrue(all(row["product_id"] != 1 for row in result["predictions"]["hybrid"]))

    def test_modified_export_fails_checksum(self):
        root, _, _, _ = load_bundle(self.pointer)
        with (root / "catalog.csv").open("a") as stream:
            stream.write("modified\n")
        with self.assertRaisesRegex(ValueError, "changed or missing"):
            load_bundle(self.pointer)

    def test_missing_bundle_shows_setup_error(self):
        with patch.dict(os.environ, {"DASHBOARD_BUNDLE": str(Path(self.temp.name) / "missing.json")}):
            app = self.app()
            self.assertFalse(app.exception)
            self.assertTrue(app.error)


if __name__ == "__main__":
    unittest.main()
