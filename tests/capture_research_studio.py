"""Browser checks against a running dashboard with real Gold data.

Run with the same INSTACART_DASHBOARD_ROOT as the Streamlit server:
python tests/capture_research_studio.py --url http://127.0.0.1:8511
"""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from playwright.sync_api import sync_playwright
from app.data_loader import gold, metrics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://127.0.0.1:8501')
    parser.add_argument('--missing-url', help='Optional server started with an empty dashboard workspace')
    parser.add_argument('--output', type=Path, default=Path('outputs/evidence/research_studio_20261016'))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    lookup, catalog = gold('recommendation_lookup'), gold('product_catalog')
    pages = ['Tổng quan', 'Hành vi', 'Mô hình & luật', 'Gợi ý', 'Hiệu năng']
    checks = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True, args=['--no-sandbox'])
        page = browser.new_page(viewport={'width': 1440, 'height': 1000}, device_scale_factor=1)
        page.goto(args.url)
        page.get_by_role('radio', name='Tổng quan', exact=True).wait_for()

        def settle():
            page.wait_for_timeout(1200)
            assert page.get_by_test_id('stException').count() == 0
            assert page.get_by_test_id('stAlert').filter(has_text='Đầu ra dữ liệu chưa đúng cấu trúc').count() == 0

        def screenshot(filename):
            page.keyboard.press('Escape')
            page.get_by_test_id('stMain').evaluate('(e) => { e.scrollTop = 0; }')
            page.wait_for_timeout(250)
            page.screenshot(path=str(args.output / filename), full_page=True)

        def choose_product(product):
            name = catalog.loc[catalog.product_id == product, 'product_name'].iloc[0]
            box = page.get_by_role('combobox')
            box.fill(f'{name} · #{product}')
            page.get_by_role('option').filter(has_text=f'{name} · #{product}').first.click()
            page.keyboard.press('Escape')
            settle()

        for index, name in enumerate(pages):
            page.get_by_test_id('stRadio').get_by_text(name, exact=True).click()
            settle()
            if name == 'Gợi ý':
                for product in lookup.iloc[0].antecedent:
                    choose_product(int(product))
                assert page.get_by_test_id('stDataFrame').count() > 0
                checks.append('Real antecedent selection produces recommendations')
            screenshot(f'{index+1:02d}_desktop.png')
            if name == 'Mô hình & luật':
                page.locator('.st-key-panel_explorer').scroll_into_view_if_needed()
                page.screenshot(path=str(args.output / 'rules_table_desktop.png'), full_page=True)
            checks.append(f'1440px: {name}')

        page.get_by_test_id('stRadio').get_by_text('Mô hình & luật', exact=True).click()
        settle()
        with page.expect_download() as download_info:
            page.get_by_role('button', name='Tải CSV luật đã lọc').click()
        download = download_info.value
        csv_path = args.output / 'filtered_rules.csv'
        download.save_as(str(csv_path))
        assert 'Antecedent,Consequent,support,confidence,lift,rule_id' in csv_path.read_text(encoding='utf-8-sig')
        checks.append('CSV download contains real antecedent and consequent')
        support = page.get_by_role('spinbutton', name='minSupport', exact=True)
        support.fill('1.0')
        support.press('Enter')
        settle()
        assert page.get_by_text('Không có luật đạt các ngưỡng đã chọn. Hãy giảm bộ lọc.').count() > 0
        checks.append('Empty filter state')
        support.fill(str(float(metrics('model_summary.json')['selected']['min_support'])))
        support.press('Enter')
        settle()
        for width in [1024, 390]:
            page.set_viewport_size({'width': width, 'height': 900})
            settle()
            screenshot(f'rules_{width}.png')
            assert page.evaluate('document.documentElement.scrollWidth <= window.innerWidth')
            checks.append(f'{width}px: no document horizontal overflow')

        page.get_by_test_id('stRadio').get_by_text('Gợi ý', exact=True).click()
        settle()
        # Clear the native multiselect and choose a real product absent from all antecedents.
        for button in page.get_by_role('button', name='Clear all').all():
            button.click()
        rule_products = {int(p) for antecedent in lookup.antecedent for p in antecedent}
        product = next(int(p) for p in catalog.product_id if int(p) not in rule_products)
        # A reload resets the basket consistently across Streamlit versions.
        page.reload()
        page.get_by_test_id('stRadio').get_by_text('Gợi ý', exact=True).click()
        settle()
        choose_product(product)
        assert page.get_by_test_id('stAlert').filter(has_text='Fallback:').count() > 0
        screenshot('fallback_mobile.png')
        checks.append('Real unmatched product triggers explicit popularity fallback')
        if args.missing_url:
            page.goto(args.missing_url)
            page.get_by_role('radio', name='Tổng quan', exact=True).wait_for()
            for name in pages:
                page.get_by_test_id('stRadio').get_by_text(name, exact=True).click()
                settle()
                assert page.get_by_test_id('stAlert').filter(has_text='Thiếu dữ liệu:').count() > 0
            screenshot('missing_data_mobile.png')
            checks.append('All five pages handle missing data in the browser')
        browser.close()
    (args.output / 'browser_checks.json').write_text(json.dumps(checks, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(checks, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
