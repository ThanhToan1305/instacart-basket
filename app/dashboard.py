"""Run with: .venv/bin/python -m streamlit run app/dashboard.py"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pandas as pd
import plotly.express as px
import streamlit as st
from app.data_loader import DataUnavailable, gold, metrics
from app.recommender import recommend

st.set_page_config(page_title='Instacart Basket Intelligence', page_icon='🛒', layout='wide')
GREEN = '#145C43'
st.markdown('''<style>
[data-testid="stSidebar"] {background-color: #ecf4ef;}
h1, h2, h3 {color: #145C43;}
[data-testid="stMetric"] {border: 1px solid #b9d8c7; border-radius: 10px; padding: 14px;}
</style>''', unsafe_allow_html=True)
st.title('Instacart Basket Intelligence')
st.caption('Khám phá hành vi mua sắm và sản phẩm mua kèm từ dữ liệu Instacart thực tế.')
try:
    evidence_context=metrics('evidence_context.json')
    st.caption(f"Bằng chứng thực nghiệm • {evidence_context['execution_mode']} • run_id={evidence_context['run_id']} • {evidence_context['created_at']}")
except DataUnavailable:
    pass
PAGES = ['Tổng quan', 'Hành vi mua sắm', 'Luật kết hợp', 'Gợi ý sản phẩm', 'Hiệu năng hệ thống']
page = st.sidebar.radio('Điều hướng', PAGES)
st.sidebar.caption('Dữ liệu tổng hợp đã lưu • Không chạy Spark khi mở dashboard')
if st.sidebar.button('Làm mới dữ liệu'):
    st.cache_data.clear()
    st.rerun()


def get(name, parquet=False):
    try:
        return gold(name) if parquet else metrics(name)
    except DataUnavailable as exc:
        st.warning(str(exc))
        if name == 'product_catalog':
            command = '.venv/bin/python app/prepare_dashboard_data.py'
        elif name in ['association_rules', 'recommendation_lookup', 'model_summary.json', 'model_comparison.csv']:
            command = '.venv/bin/python src/pipeline/05_train_fpgrowth.py\n.venv/bin/python src/pipeline/06_evaluate_rules.py'
        elif name == 'silver_profile.json':
            command = '.venv/bin/python src/pipeline/03_build_thưsilver.py'
        elif name == 'dashboard_manifest.json':
            command = '.venv/bin/python app/prepare_dashboard_data.py'
        elif name == 'raw_profile.json':
            command = '.venv/bin/python src/pipeline/01_validate_raw.py\n.venv/bin/python src/pipeline/02_ingest_bronze.py'
        else:
            command = '.venv/bin/python src/pipeline/04_eda.py'
        st.info('Cần khôi phục đầu ra pipeline tương ứng. Kiểm tra checkpoint/đầu ra hiện có trước khi chạy lại.')
        st.code(command, language='bash')
        return None


def chart(name, x, y, title, labels, horizontal=False, percent=False):
    data = get(name+'.csv')
    if data is None:
        return
    fig = px.bar(data, x=y if horizontal else x, y=x if horizontal else y,
        orientation='h' if horizontal else 'v', title=title, labels=labels,
        color_discrete_sequence=[GREEN])
    fig.update_layout(template='plotly_white')
    if horizontal:
        fig.update_yaxes(autorange='reversed')
    if percent:
        fig.update_xaxes(tickformat='.0%') if horizontal else fig.update_yaxes(tickformat='.0%')
    st.plotly_chart(fig, width='stretch', key=page+'_'+name)


def overview():
    summary = get('eda_summary.json')
    if summary is not None:
        if summary.get('status') != 'PASS':
            st.warning('KPI chưa có trạng thái PASS; hãy kiểm tra pipeline EDA.')
        kpi = summary['kpi']
        for columns, entries in [(st.columns(3), [('Tổng người dùng','total_users'),('Tổng đơn hàng','total_orders'),('Lượt mua quan sát','total_order_items')]),
                                 (st.columns(3), [('Sản phẩm trong danh mục','total_products'),('Kích thước giỏ trung bình','avg_basket_size'),('Tỷ lệ mua lại','reorder_rate')])]:
            for col, (label,key) in zip(columns, entries):
                value = kpi[key]
                text = f'{value:.2%}' if key == 'reorder_rate' else f'{value:.2f}' if key == 'avg_basket_size' else f'{value:,}'
                col.metric(label, text)
        st.caption(f"Đơn hàng gồm cả test; giỏ/lượt mua chỉ gồm prior + train. {kpi['orders_without_observed_items']:,} đơn test không có chi tiết sản phẩm. Reorder = lượt reordered / lượt mua quan sát.")
    left,right = st.columns(2)
    with left: chart('orders_by_hour','order_hour_of_day','count','Đơn hàng theo giờ',{'order_hour_of_day':'Giờ','count':'Số đơn'})
    with right: chart('top_20_products','product_name','count','20 sản phẩm mua nhiều nhất',{'product_name':'Sản phẩm','count':'Lượt mua'},True)
    model = get('model_summary.json')
    if model and 'selected' in model:
        c = model['selected']
        st.subheader('Chất lượng gợi ý trên train')
        a,b,d = st.columns(3)
        a.metric('Luật hữu dụng',f"{c['useful_rules']:,}")
        b.metric('Coverage',f"{c['coverage']:.2%}")
        d.metric('HitRate@5',f"{c['hitrate_at_5']:.2%}")
        st.caption('Đánh giá nội bộ: train cũng được dùng chọn cấu hình; không phải phép kiểm thử cuối độc lập.')


def behavior():
    st.subheader('Hành vi mua sắm')
    a,b = st.columns(2)
    with a: chart('orders_by_hour','order_hour_of_day','count','Đơn hàng theo giờ',{'order_hour_of_day':'Giờ','count':'Số đơn'})
    with b:
        chart('orders_by_dow','order_dow','count','Đơn hàng theo mã ngày',{'order_dow':'Mã ngày 0–6','count':'Số đơn'})
        st.caption('Mã ngày đã ẩn danh; không quy đổi thành thứ trong tuần.')
    with a: chart('top_20_products','product_name','count','Top sản phẩm',{'product_name':'Sản phẩm','count':'Lượt mua'},True)
    with b: chart('top_15_departments','department','count','Top department',{'department':'Department','count':'Lượt mua'},True)
    with a: chart('basket_size_distribution','basket_size','count','Phân phối kích thước giỏ',{'basket_size':'Số sản phẩm trong giỏ','count':'Số giỏ'})
    with b: chart('reorder_by_department','department','reorder_rate','Tỷ lệ mua lại theo department',{'department':'Department','reorder_rate':'Tỷ lệ mua lại'},True,True)


def rule_page():
    st.subheader('Luật kết hợp')
    st.caption('Bộ lọc áp dụng trên mô hình Gold đã chọn; giảm ngưỡng không tạo thêm luật chưa được lưu.')
    rules = get('association_rules',True)
    if rules is None or rules.empty:
        if rules is not None: st.info('Chưa có luật trong Gold.')
        return
    a,b,c = st.columns(3)
    support = a.number_input('minSupport',min_value=0.0,max_value=1.0,value=0.001,step=0.0005,format='%.4f')
    confidence = b.slider('minConfidence',0.0,1.0,0.20,0.05)
    lift = c.number_input('minLift',min_value=1.0,value=1.0,step=0.5)
    filtered = rules[(rules.support>=support)&(rules.confidence>=confidence)&(rules.lift>=lift)].copy()
    filtered['Luật'] = [', '.join(r.antecedent_names)+' → '+', '.join(r.consequent_names)
                        for r in filtered.itertuples()]
    filtered = filtered.sort_values(['lift','support','rule_id'],ascending=[False,False,True])
    st.metric('Luật đạt bộ lọc',len(filtered))
    visible = filtered[['Luật','support','confidence','lift','rule_id']]
    st.dataframe(visible,width='stretch',hide_index=True)
    st.download_button('Tải CSV luật đã lọc',visible.to_csv(index=False).encode('utf-8-sig'),'instacart_rules.csv','text/csv')
    if filtered.empty:
        st.info('Không có luật đạt các ngưỡng đã chọn. Hãy giảm bộ lọc.')
        return
    top = filtered.head(20).copy()
    top['Nhãn'] = [f'{i}. {label}' for i,label in enumerate(top['Luật'],1)]
    fig = px.bar(top,x='lift',y='Nhãn',orientation='h',title='Top 20 luật theo lift (sau bộ lọc support)',color_discrete_sequence=[GREEN])
    fig.update_yaxes(autorange='reversed');fig.update_layout(height=800,template='plotly_white')
    st.plotly_chart(fig,width='stretch',key='rules_top_lift')
    scatter = px.scatter(filtered,x='confidence',y='lift',size='support',hover_name='Luật',
        title='Confidence–lift của luật đã lọc',color_discrete_sequence=[GREEN],labels={'confidence':'Confidence','lift':'Lift','support':'Support'})
    scatter.update_layout(template='plotly_white')
    st.plotly_chart(scatter,width='stretch',key='rules_confidence_lift')


def recommendation_page():
    st.subheader('Gợi ý sản phẩm mua kèm')
    catalog = get('product_catalog',True)
    popular = get('top_20_products.csv')
    lookup = get('recommendation_lookup',True)
    if catalog is None:
        return
    names = dict(zip(catalog.product_id.astype(int),catalog.product_name))
    options = catalog.sort_values(['product_name','product_id']).product_id.astype(int).tolist()
    selected = st.multiselect('Sản phẩm đang có trong giỏ',options,format_func=lambda x:f'{names[x]} · #{x}')
    st.caption('Khớp toàn bộ antecedent, loại sản phẩm đã có, tối đa năm sản phẩm duy nhất. Điểm là confidence của luật tốt nhất; lift và support dùng để phá hòa.')
    if not selected:
        st.info('Chọn ít nhất một sản phẩm để nhận gợi ý.')
        return
    if lookup is None:
        st.warning('Thiếu lookup: chỉ có thể dùng fallback phổ biến nếu dữ liệu này có sẵn.')
    if popular is None and lookup is None:
        return
    result = recommend(selected,lookup if lookup is not None else pd.DataFrame(),
        popular if popular is not None else pd.DataFrame(columns=['product_id','product_name','count']),catalog)
    if result.empty:
        st.info('Không còn sản phẩm phù hợp trong luật hoặc danh sách phổ biến đã lưu.')
        return
    if result.iloc[0]['source']=='Fallback phổ biến':
        st.warning('Fallback: không có luật khớp hoặc lookup chưa có. Gợi ý theo lượt mua trong top 20 phổ biến đã lưu; không có điểm confidence/support/lift.')
    else:
        st.success(f'{len(result)} sản phẩm từ luật kết hợp.')
    display = result.rename(columns={'product_name':'Sản phẩm','score':'Điểm (confidence)','source':'Nguồn',
        'source_rule':'Luật nguồn','support':'Support','confidence':'Confidence','lift':'Lift','purchase_count':'Lượt mua'})
    st.dataframe(display.drop(columns='rule_id'),width='stretch',hide_index=True)


def performance():
    st.subheader('Hiệu năng hệ thống')
    raw = get('raw_profile.json');silver = get('silver_profile.json');eda = get('eda_summary.json');model = get('model_summary.json')
    times=[];counts=[]
    for label,report in [('Silver',silver),('KPI/EDA',eda),('FP-Growth/đánh giá',model)]:
        if report is None: continue
        for name,seconds in report.get('stage_seconds',{}).items():
            times.append({'Pipeline':label,'Stage':name,'Giây':seconds})
    if model:
        for r in model.get('comparisons',[]):
            if r['status']=='PASS' and r['min_confidence']==min(model['config']['min_confidences']):
                times.append({'Pipeline':'FP-Growth','Stage':f"fit support={r['min_support']}",'Giây':r['training_seconds']})
        prep = model.get('preparation',{})
        counts.append({'Stage':'Chuẩn bị giỏ prior','Đầu vào':prep.get('prior_baskets'),'Đầu ra':prep.get('training_baskets')})
        for name,v in model.get('exports',{}).items():
            before = prep.get('training_baskets') if name == 'frequent_itemsets' else model['selected']['association_rules'] if name == 'association_rules' else model['final_rule_count']
            counts.append({'Stage':'Gold/'+name,'Đầu vào':before,'Đầu ra':v['rows']})
    if silver:
        for name,v in silver.get('tables',{}).items():
            before = silver.get('bronze_validation',{}).get(name,{}).get('row_count')
            if name == 'order_items':
                before = sum(silver['bronze_validation'][n]['row_count'] for n in ['order_products_prior','order_products_train'])
            elif name == 'order_items_enriched':
                before = silver['tables']['order_items']['rows']
            elif name == 'baskets':
                before = silver['tables']['order_items']['rows']
            counts.append({'Stage':'Silver/'+name,'Đầu vào':before,'Đầu ra':v['rows']})
    manifest = get('dashboard_manifest.json')
    if manifest:
        st.subheader('Cấu hình Spark của project')
        st.json(manifest['spark_project_config'])
        st.caption('Snapshot từ config/settings.yaml khi chuẩn bị dữ liệu dashboard; metrics Silver không lưu cấu hình runtime.')
    if raw:
        for name,v in raw.get('tables',{}).items():
            counts.append({'Stage':'Raw → Bronze/'+name,'Đầu vào':v.get('row_count'),'Đầu ra':v.get('bronze_rows')})
            for field in ['validation_seconds','ingestion_seconds']:
                if field in v:
                    times.append({'Pipeline':'Raw/Bronze','Stage':name+'/'+field,'Giây':v[field]})
    if times: st.dataframe(pd.DataFrame(times),hide_index=True,width='stretch')
    st.caption('Mỗi support chỉ fit một lần; thời gian lặp ở ba dòng confidence không cộng ba lần. Đoạn resume không bao gồm fit đã lưu. Ô trống ở số dòng đầu vào nghĩa là metrics không ghi chỉ số đó.')
    if counts: st.dataframe(pd.DataFrame(counts),hide_index=True,width='stretch')
    if model:
        st.subheader('Cấu hình FP-Growth')
        st.json(model.get('config',{}))
        st.caption('Task 4 ghi đè driver memory/task concurrency riêng; cấu hình Spark chung của Silver có thể khác.')
        st.json({'driver_memory':model['config'].get('driver_memory'),'task_cpus':model['config'].get('task_cpus'),
                 'num_partitions':model['config'].get('num_partitions')})
        st.info(model.get('timing_notes','Thời gian resume chỉ bao gồm phiên tiếp tục cuối.'))
    comparison = get('model_comparison.csv')
    if comparison is not None:
        st.subheader('So sánh mô hình')
        st.dataframe(comparison,hide_index=True,width='stretch')
        st.caption('SKIPPED_OUT_OF_MEMORY: cấu hình bỏ qua do bằng chứng lỗi RAM; không thay ô thiếu bằng số 0.')
    if raw:
        st.subheader('Trạng thái kiểm tra raw')
        st.write(raw.get('status','Xem profile raw'))


try:
    {'Tổng quan':overview,'Hành vi mua sắm':behavior,'Luật kết hợp':rule_page,
     'Gợi ý sản phẩm':recommendation_page,'Hiệu năng hệ thống':performance}[page]()
except (KeyError,ValueError,TypeError) as exc:
    st.error(f'Đầu ra dữ liệu chưa đúng cấu trúc: {exc}. Kiểm tra lại báo cáo và pipeline tạo dữ liệu cho trang này.')
