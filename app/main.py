"""Vietnamese dashboard for verified Instacart experiment artifacts."""
import json
import os
import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from dashboard_bundle import load_bundle
from recommendation_core import Recommender, MODELS, METRICS, rule_key

NAMES = {"popularity": "Popularity", "fpgrowth_rule_only": "FP-Growth thuần", "hybrid": "Hybrid"}
PAGES = ["Tổng quan", "Hành vi mua sắm", "Luật kết hợp", "Thử giỏ hàng", "Đánh giá & tái lập"]

st.set_page_config(page_title="Giỏ hàng thông minh · Nhóm 10", page_icon="🛒", layout="wide")
st.markdown("""<style>
.block-container{padding-top:2rem;max-width:1440px} [data-testid="stMetric"]{background:white;border:1px solid #e4eae7;border-radius:12px;padding:16px}
[data-testid="stSidebar"]{border-right:1px solid #e4eae7} h1{letter-spacing:-.035em} h3{letter-spacing:-.02em}
div[data-testid="stVerticalBlockBorderWrapper"]{border-radius:12px}
</style>""", unsafe_allow_html=True)

pointer = Path(os.environ.get("DASHBOARD_BUNDLE", str(Path(__file__).resolve().parents[1] / "outputs/dashboard/latest.json")))
try:
    root, data, serving, manifest = load_bundle(pointer)
except Exception as error:
    st.title("Giỏ hàng thông minh")
    st.error("Chưa có bộ dữ liệu dashboard hợp lệ. Hãy chạy export_dashboard.py rồi tải lại trang.")
    st.caption(str(error))
    st.stop()


@st.cache_data
def read_csv(path, checksum, catalog=False):
    return pd.read_csv(path, encoding="utf-8-sig", keep_default_na=not catalog)


def table(name):
    filename = "eda/" + name + ".csv"
    return read_csv(str(root / filename), manifest["files"][filename])


catalog = read_csv(str(root / "catalog.csv"), manifest["files"]["catalog.csv"], True)
lookup = catalog.set_index("product_id")["product_name"].to_dict()
engine = Recommender(serving["rules"], serving["popularity"])
test_metrics = data["test_report"]["test_metrics"]


def product_name(pid):
    return str(lookup.get(pid, f"Sản phẩm #{pid}"))


def number(value):
    return f"{value:,}".replace(",", ".")


def chart(frame, x, y, title, horizontal=False, percent=False):
    if horizontal:
        figure = alt.Chart(frame).mark_bar(color="#159567", cornerRadiusEnd=4).encode(
            y=alt.Y(x + ":N", sort="-x", title=None), x=alt.X(y + ":Q", title=title),
            tooltip=[alt.Tooltip(x + ":N"), alt.Tooltip(y + ":Q", format=",.0f")])
    else:
        figure = alt.Chart(frame).mark_bar(color="#159567", cornerRadiusTopLeft=4, cornerRadiusTopRight=4).encode(
            x=alt.X(x + ":O", title=None, sort="ascending"), y=alt.Y(y + ":Q", title=title), tooltip=[x, y])
    st.altair_chart(figure.properties(height=300), use_container_width=True)


def overview():
    st.title("Từ giao dịch đến gợi ý mua kèm")
    st.write("Phân tích hành vi mua sắm với Apache Spark, HDFS và FP-Growth trên dữ liệu Instacart.")
    k = data["kpis"]
    cols = st.columns(4)
    for col, label, value in zip(cols, ["Người dùng", "Đơn hàng", "Giỏ có nhãn", "Sản phẩm trong danh mục"],
                                  [k["distinct_users"], k["all_orders"], k["labeled_orders"], k["catalog_products"]]):
        col.metric(label, number(value))
    st.caption("Các KPI mô tả toàn bộ dữ liệu. Giỏ có nhãn gồm prior + train; đơn test thiếu chi tiết sản phẩm vẫn được tính trong tổng đơn.")
    left, right = st.columns([1.25, 1])
    with left:
        st.subheader("Mua sắm tập trung vào sản phẩm nào?")
        top = table("product_popularity_eda").sort_values(["order_count", "product_id"], ascending=[False, True]).head(10)
        chart(top, "product_name", "order_count", "Số giỏ chứa sản phẩm", True)
    with right:
        st.subheader("Thí nghiệm hiện tại")
        st.metric("Giỏ dùng để học", number(data["model_report"]["train_baskets"]))
        st.write(f"Mẫu gồm **{number(data['sample']['users'])} người dùng**, giữ đầy đủ lịch sử quan sát được.")
        selected = data["model_report"]["selected"]
        st.write(f"Cấu hình: support **{selected['minSupport']}**, confidence **{selected['minConfidence']}**; **{selected['rule_count']} luật**.")
        st.info("Mô hình được chọn bằng validation. Điểm test đo hoàn thiện một giỏ đã quan sát được.")


def behavior():
    st.title("Hành vi mua sắm")
    st.caption("Phạm vi EDA: toàn bộ orders cho thời điểm đặt hàng; prior + train có nhãn cho giỏ và lượt sản phẩm.")
    a, b, c = st.columns(3)
    a.metric("Sản phẩm trung bình / giỏ", f"{data['kpis']['avg_labeled_basket_size']:.2f}")
    b.metric("Giỏ lớn nhất", number(data["kpis"]["max_labeled_basket_size"]))
    c.metric("Lượt sản phẩm mua lại", f"{data['kpis']['item_reorder_rate']:.2%}")
    st.caption("Tỷ lệ mua lại tính theo cờ reordered của lượt sản phẩm, không phải tỷ lệ khách hàng quay lại.")
    left, right = st.columns(2)
    with left:
        st.subheader("Giờ đặt hàng")
        chart(table("orders_by_hour"), "order_hour_of_day", "order_count", "Số đơn")
    with right:
        st.subheader("Mã ngày đặt hàng")
        chart(table("orders_by_dow"), "order_dow", "order_count", "Số đơn")
        st.caption("Giữ nguyên mã ngày 0–6 của nguồn dữ liệu.")
    left, right = st.columns(2)
    with left:
        st.subheader("Kích thước giỏ")
        slider_max = max(2, int(data["kpis"]["max_labeled_basket_size"]))
        maximum = st.slider("Hiển thị giỏ tối đa", 1, slider_max, min(40, slider_max))
        sizes = table("basket_size_distribution")
        chart(sizes[sizes.basket_size <= maximum], "basket_size", "order_count", "Số giỏ")
        st.caption("Bộ lọc chỉ thay phạm vi biểu đồ; các KPI vẫn tính trên toàn bộ giỏ có nhãn.")
    with right:
        st.subheader("Ngành hàng phổ biến")
        chart(table("department_activity_eda").nlargest(12, "item_occurrences"), "department", "item_occurrences", "Lượt sản phẩm", True)


def association_rules():
    st.title("Luật kết hợp")
    st.write("Khám phá các quan hệ mua kèm học từ giỏ prior của train users.")
    a, b, c = st.columns(3)
    a.metric("Luật của mô hình", len(serving["rules"]))
    b.metric("Tập phổ biến", data["model_report"]["selected"]["frequent_itemsets"])
    c.metric("Coverage rule-only trên test", f"{test_metrics['fpgrowth_rule_only']['coverage_at_5']:.2%}")
    left, right = st.columns(2)
    confidence = left.slider("Confidence tối thiểu để xem", 0.0, 1.0, float(serving["selected"]["minConfidence"]), .05)
    search = right.text_input("Tìm sản phẩm trong luật", key="rule_search")
    rows = []
    for rule in sorted(serving["rules"], key=rule_key):
        first = ", ".join(product_name(p) for p in rule["antecedent"])
        last = product_name(rule["consequent"][0])
        if rule["confidence"] >= confidence and search.casefold() in (first + " " + last).casefold():
            rows.append({"Đã có trong giỏ": first, "Gợi ý mua kèm": last,
                         "Confidence (%)": rule["confidence"] * 100, "Lift": rule["lift"], "Support (%)": rule["support"] * 100})
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    st.caption(f"Hiển thị {len(rows)} luật. Bộ lọc này không thay đổi mô hình demo hoặc kết quả đánh giá.")
    with st.expander("Đọc support, confidence và lift"):
        st.write("Support: tỷ lệ giỏ train chứa cả hai vế. Confidence: tỷ lệ giỏ có vế phải trong các giỏ chứa vế trái. Lift so sánh đồng xuất hiện với mức kỳ vọng khi độc lập. Các số này không phải xác suất một gợi ý chắc chắn đúng cho người dùng mới.")


def basket_demo():
    st.title("Thử giỏ hàng")
    st.write("Chọn sản phẩm đang có trong giỏ để nhận tối đa 5 gợi ý và xem nguồn của từng gợi ý.")
    st.session_state.setdefault("cart", [])
    if st.button("Nạp ví dụ từ một luật", disabled=not serving["rules"], key="load_example"):
        st.session_state["cart"] = sorted(serving["rules"], key=rule_key)[0]["antecedent"]
    st.caption("Nút ví dụ lấy vế trái một luật để minh họa; đây không phải ca test ngẫu nhiên.")
    query = st.text_input("Tìm theo tên sản phẩm hoặc ID", key="product_search")
    selected = [pid for pid in st.session_state["cart"] if pid in lookup]
    if query.strip():
        q = query.strip()
        filtered = catalog[catalog.product_id.eq(int(q))] if q.isdigit() else catalog[catalog.product_name.str.contains(q, case=False, regex=False)]
        options = filtered.product_id.head(200).tolist()
    else:
        options = [row["product_id"] for row in serving["popularity"][:100]]
    options = list(dict.fromkeys(selected + options))
    cart = st.multiselect("Sản phẩm trong giỏ", options, format_func=lambda p: f"{product_name(p)} · #{p}", key="cart")
    st.caption("Khi chưa tìm kiếm, danh sách hiện 100 sản phẩm phổ biến trong train. Tìm kiếm cho phép chọn sản phẩm khác trong danh mục.")
    if st.button("Gợi ý sản phẩm", type="primary", key="recommend"):
        if not cart:
            st.info("Chọn ít nhất một sản phẩm trước khi gợi ý.")
        else:
            st.session_state["demo_result"] = {"context": sorted(cart), "model": data["model_run"], "predictions": engine.recommend(cart, details=True)}
    result = st.session_state.get("demo_result")
    if not result or result["context"] != sorted(cart) or result["model"] != data["model_run"]:
        return
    tabs = st.tabs(["Hybrid", "FP-Growth thuần", "Popularity"])
    for tab, name in zip(tabs, ["hybrid", "fpgrowth_rule_only", "popularity"]):
        with tab:
            predictions = result["predictions"][name]
            if not predictions:
                st.info("Không có luật phù hợp với giỏ này.")
            for rank, row in enumerate(predictions, 1):
                with st.container(border=True):
                    st.markdown(f"**{rank}. {product_name(row['product_id'])}**")
                    if row["source"] == "rule":
                        first = ", ".join(product_name(p) for p in row["antecedent"])
                        st.caption(f"Từ luật: {first} → {product_name(row['product_id'])}")
                        st.write(f"Confidence {row['confidence']:.2%} · Lift {row['lift']:.2f} · Support {row['support']:.2%}")
                    else:
                        st.caption(f"Từ Popularity · xuất hiện trong {number(row['frequency'])} giỏ train")
    st.caption("Demo sử dụng đúng bộ luật và thứ hạng Popularity của mô hình đã được đánh giá.")


def evaluation():
    st.title("Đánh giá & tái lập")
    p, h = test_metrics["popularity"], test_metrics["hybrid"]
    n = h["cases"]
    a, b, c = st.columns(3)
    a.metric("Hybrid: ca có gợi ý trúng", f"{round(h['hit_rate_at_5'] * n)}/{n}")
    b.metric("Popularity: ca có gợi ý trúng", f"{round(p['hit_rate_at_5'] * n)}/{n}")
    c.metric("Chênh lệch HitRate@5", f"{(h['hit_rate_at_5'] - p['hit_rate_at_5']) * 100:+.3f} điểm %")
    st.info("Chênh lệch trên mẫu test này chưa chứng minh ưu thế tổng quát. Thí nghiệm dùng một mẫu người dùng, một seed và một lần che giỏ.")
    stage = st.radio("Tập kết quả", ["Test", "Validation"], horizontal=True, key="score_stage")
    scores = test_metrics if stage == "Test" else data["model_report"]["validation_metrics"]
    rows = [{"Phương pháp": NAMES[name], "Số ca": values["cases"], "HitRate@5 (%)": 100 * values["hit_rate_at_5"],
             "Precision@5 (%)": 100 * values["precision_at_5"], "Recall@5 (%)": 100 * values["recall_at_5"],
             "NDCG@5": values["ndcg_at_5"], "Coverage@5 (%)": 100 * values["coverage_at_5"],
             "Đủ 5 gợi ý (%)": 100 * values["full_list_rate_at_5"]} for name, values in scores.items()]
    st.dataframe(pd.DataFrame(rows).round(4), hide_index=True, use_container_width=True)
    st.caption("Coverage là tỷ lệ ca nhận ít nhất một gợi ý. Precision chia cố định cho 5, kể cả danh sách ngắn. FP thuần dùng chung cấu hình được chọn cho Hybrid.")
    pair = data["paired_test"]
    st.write(f"Trên test: cả hai cùng trúng **{pair['both_hit']}** ca; chỉ Hybrid trúng **{pair['hybrid_only_hit']}**; chỉ Popularity trúng **{pair['popularity_only_hit']}**.")
    with st.expander("Protocol và nguồn kết quả"):
        st.write("Bài toán hoàn thiện giỏ: context là đầu vào, target là phần bị ẩn. Mô hình chỉ học giỏ prior của train users; validation chọn cấu hình, test đánh giá mô hình đã đóng băng. Giỏ một sản phẩm bị loại; target chưa gặp vẫn được giữ.")
        st.json({"model_run": data["model_run"], "split_run": data["source_split_run"], "preparation_run": data["source_preparation_run"],
                 "selected": serving["selected"], "protocol_sha256": serving["protocol_sha256"], "serving_sha256": data["serving_sha256"]})
    with st.expander("Grid validation đã thực hiện"):
        st.dataframe(pd.DataFrame([{"Support": r["minSupport"], "Confidence": r["minConfidence"], "Số luật": r["rule_count"],
                                  "Hybrid HitRate@5 (%)": r["validation_metrics"]["hybrid"]["hit_rate_at_5"] * 100}
                                 for r in data["model_report"]["candidates"]]), hide_index=True, use_container_width=True)
    st.download_button("Tải báo cáo test JSON", json.dumps(data["test_report"], indent=2), file_name="test_report.json", mime="application/json")


with st.sidebar:
    st.markdown("### 🛒 Instacart Lab")
    st.caption("NHÓM 10 · BIG DATA")
    page = st.radio("Điều hướng", PAGES, key="page", label_visibility="collapsed")
    st.divider()
    st.caption("Mô hình đã đánh giá")
    st.write(f"{len(serving['rules'])} luật · Top 5 gợi ý")
    st.caption("Spark → HDFS → Streamlit")
if data.get("data_kind") == "synthetic":
    st.warning("Dữ liệu kiểm thử giả lập — không phải kết quả Instacart thực tế.")
st.caption("PHÂN TÍCH HÀNH VI MUA SẮM & GỢI Ý SẢN PHẨM MUA KÈM")
{PAGES[0]: overview, PAGES[1]: behavior, PAGES[2]: association_rules, PAGES[3]: basket_demo, PAGES[4]: evaluation}[page]()
