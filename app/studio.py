"""Research Studio shell, native navigation and shared chart styling."""
from pathlib import Path
import plotly.io as pio
import streamlit as st

PAGES = ['Tổng quan', 'Hành vi', 'Mô hình & luật', 'Gợi ý', 'Hiệu năng']
ICONS = ['dashboard', 'bar_chart', 'hub', 'shopping_basket', 'monitor_heart']
COPY = [
    ('RESEARCH OVERVIEW', 'Hiểu giỏ hàng. Khám phá hành vi.', 'Bức tranh mua sắm từ dữ liệu Instacart thực tế.'),
    ('BEHAVIOR ANALYSIS', 'Nhịp điệu của mỗi giỏ hàng.', 'Khám phá thời điểm mua sắm, sản phẩm và hành vi mua lại.'),
    ('MODEL EXPLORER', 'Từ giao dịch đến tri thức.', 'So sánh cấu hình và khám phá các luật mua kèm.'),
    ('BASKET LAB', 'Khám phá sản phẩm mua kèm.', 'Chọn giỏ hàng để thử gợi ý Top-5 từ các luật đã lưu.'),
    ('SYSTEM OBSERVATORY', 'Quan sát hệ thống. Đọc kết quả.', 'Cấu hình, thời gian xử lý và đối soát dữ liệu pipeline.'),
]
ACCENT = '#5266BA'


def select_page(page):
    st.session_state['studio_page'] = page


def shell():
    st.set_page_config(page_title='Instacart / Research Studio', page_icon='◈', layout='wide', initial_sidebar_state='expanded')
    st.markdown('<style>' + Path(__file__).with_name('studio.css').read_text(encoding='utf-8') + '</style>', unsafe_allow_html=True)
    pio.templates['studio'] = pio.templates['plotly_white']
    pio.templates['studio'].layout.update(
        font=dict(family='Arial, sans-serif', color='#6D7790', size=12),
        colorway=[ACCENT, '#8B99D3', '#B7C1E6'], paper_bgcolor='white', plot_bgcolor='white',
        margin=dict(l=24, r=24, t=60, b=40),
        xaxis=dict(gridcolor='#DFE4EF', zeroline=False),
        yaxis=dict(gridcolor='#DFE4EF', zeroline=False),
        title=dict(font=dict(color='#24314C', size=18)),
    )
    pio.templates.default = 'studio'
    st.session_state.setdefault('studio_page', PAGES[0])
    with st.sidebar:
        st.button(' ', icon=':material/layers:', help='Instacart Research Studio', key='studio_home', width='stretch',
                  on_click=select_page, args=(PAGES[0],))
        for name, icon in zip(PAGES, ICONS):
            st.button(' ', icon=f':material/{icon}:', help=name, key='rail_'+icon, width='stretch',
                      type='primary' if st.session_state.studio_page == name else 'secondary',
                      on_click=select_page, args=(name,))
        st.button(' ', icon=':material/refresh:', help='Làm mới dữ liệu', on_click=st.cache_data.clear, width='stretch')
    st.markdown('<div class="studio-header"><span>INSTACART <span class="muted">/ Research Studio</span></span><span class="studio-tag">Local Spark</span></div>', unsafe_allow_html=True)
    index = PAGES.index(st.session_state.studio_page)
    eyebrow, title, description = COPY[index]
    st.markdown(f'<div class="studio-intro"><div class="eyebrow">{eyebrow}</div><h1>{title}</h1><p>{description}</p><span class="validation-tag">Validation nội bộ</span></div>', unsafe_allow_html=True)
    return st.radio('Trang nghiên cứu', PAGES, horizontal=True, key='studio_page', label_visibility='collapsed')


def model_panel(model):
    with st.container(border=True, key='selected_model'):
        st.caption('CẤU HÌNH ĐƯỢC CHỌN')
        st.subheader('FP-Growth')
        if not model or not model.get('selected'):
            st.info('Chưa có cấu hình mô hình đã lưu.')
            return
        selected = model['selected']
        for label, key in [('minSupport', 'min_support'), ('minConfidence', 'min_confidence'),
                           ('Frequent itemsets', 'frequent_itemsets'), ('Số luật', 'association_rules')]:
            value = selected.get(key)
            text = '—' if value is None else f'{value:,}'
            st.markdown(f'<div class="model-setting"><span>{label}</span><strong>{text}</strong></div>', unsafe_allow_html=True)
        a, b = st.columns(2)
        for col, label, key in [(a, 'Coverage', 'coverage'), (b, 'HitRate@5', 'hitrate_at_5')]:
            value = selected.get(key)
            col.metric(label, '—' if value is None else f'{value:.2%}')
        st.caption('Train dùng để chọn cấu hình và validation nội bộ.')
