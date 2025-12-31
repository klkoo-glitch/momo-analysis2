import streamlit as st
import pandas as pd
import os
import shutil
from datetime import datetime, timedelta
import io

# 1. 페이지 설정
st.set_page_config(page_title="모모유부 엑셀 데이터 추출기", layout="wide")
st.title("📂 모모유부 지점별 통합 데이터 추출 (엑셀 추출용)")

# 파일 경로
file_path = '지점별 샘플러스 데이터_2025.12.29.xlsx'
DUPLICATE_LIMIT = 30 

@st.cache_data(ttl=600)
def process_data_for_excel():
    if not os.path.exists(file_path): 
        return None, "FILE_NOT_FOUND"
    
    temp_path = "temp_export_final.xlsx"
    try:
        shutil.copyfile(file_path, temp_path)
        excel = pd.ExcelFile(temp_path)
        combined_data = []
        
        def unify_name(x):
            txt = str(x)
            if '강남구청' in txt: return '강남구청'
            if '기흥' in txt: return '기흥'
            if '여의도' in txt or '브라이튼' in txt: return '여의도'
            if '목동' in txt: return '목동'
            if '원주' in txt: return '원주'
            if '강남' in txt: return '강남'
            return "기타"

        for sheet in excel.sheet_names:
            if any(x in sheet for x in ['요약', '공식']): continue
            df_sheet = pd.read_excel(temp_path, sheet_name=sheet, skiprows=3)
            if df_sheet.empty: continue
            
            is_shifted = df_sheet['가맹점명'].astype(str).str.match(r'\d{4}[-./]\d{2}[-./]\d{2}')
            
            normal = df_sheet[~is_shifted].copy()
            if not normal.empty:
                req = ['카드번호', '거래금액', '거래일자', '거래시간', '가맹점명', '거래유형']
                cols = [c for c in req if c in normal.columns]
                tmp = normal[cols].copy()
                tmp['가맹점명'] = tmp['가맹점명'].apply(unify_name)
                combined_data.append(tmp)
            
            shifted = df_sheet[is_shifted].copy()
            if not shifted.empty:
                sh = pd.DataFrame()
                sh['카드번호'] = shifted['체크']; sh['거래금액'] = shifted['봉사료']
                sh['거래일자'] = shifted['가맹점명']; sh['거래시간'] = shifted['발급사']
                sh['거래유형'] = shifted['카드번호']; sh['가맹점명'] = unify_name(sheet)
                combined_data.append(sh)
        
        full_df = pd.concat(combined_data, sort=False).reset_index(drop=True)
        full_df['거래금액'] = pd.to_numeric(full_df['거래금액'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        # 취소분 보정 로직
        full_df['net_sales'] = full_df.apply(lambda x: -x['거래금액'] if str(x.get('거래유형', '')) == '취소' else x['거래금액'], axis=1)
        
        full_df['datetime'] = pd.to_datetime(full_df['거래일자'].astype(str).str.split(' ').str[0] + ' ' + full_df['거래시간'].astype(str).fillna('00:00:00'), errors='coerce')
        full_df = full_df.dropna(subset=['datetime', '카드번호'])
        full_df = full_df.sort_values(['가맹점명', '카드번호', 'datetime'])
        
        # 중복 제거 (30분)
        full_df['time_diff'] = full_df.groupby(['가맹점명', '카드번호'])['datetime'].diff().dt.total_seconds() / 60.0
        full_df = full_df[~((full_df['time_diff'] <= DUPLICATE_LIMIT) & (full_df['time_diff'].notnull()))]
        
        # 고객 행동 데이터
        full_df['visit_no'] = full_df.groupby(['가맹점명', '카드번호']).cumcount() + 1
        full_df['first_v'] = full_df.groupby(['가맹점명', '카드번호'])['datetime'].transform('min')
        full_df['last_v'] = full_df.groupby(['가맹점명', '카드번호'])['datetime'].transform('max')
        full_df['total_v_all'] = full_df.groupby(['가맹점명', '카드번호'])['datetime'].transform('count')
        
        second_v = full_df[full_df['visit_no'] == 2][['가맹점명', '카드번호', 'datetime']]
        second_v.columns = ['가맹점명', '카드번호', 'second_date']
        full_df = full_df.merge(second_v, on=['가맹점명', '카드번호'], how='left')
        full_df['연월'] = full_df['datetime'].dt.strftime('%Y-%m')
        
        return full_df, "SUCCESS"
    except Exception as e: return None, str(e)

df_main, status = process_data_for_excel()

if status == "SUCCESS" and df_main is not None:
    stores = [s for s in sorted(df_main['가맹점명'].unique()) if s != "기타"]
    data_end_date = df_main['datetime'].max()
    all_months = sorted(df_main['연월'].unique())

    all_store_metrics = []

    for s in stores:
        s_data = df_main[df_main['가맹점명'] == s]
        
        metrics_rows = {
            "매출액": {"매장": s, "항목": "매출액"},
            "전체 방문자수": {"매장": s, "항목": "전체 방문자수"},
            "신규 방문자수": {"매장": s, "항목": "신규 방문자수"},
            "신규비율(%)": {"매장": s, "항목": "신규비율(%)"},
            "재방문자수": {"매장": s, "항목": "재방문자수"},
            "재방문자비율(%)": {"매장": s, "항목": "재방문자비율(%)"},
            "잠재 단골(2~3회)": {"매장": s, "항목": "잠재 단골(2~3회)"},
            "충성고객(4회이상)": {"매장": s, "항목": "충성고객(4회이상)"},
            "충성고객비율(%)": {"매장": s, "항목": "충성고객비율(%)"},
            "전체 전환율(%)": {"매장": s, "항목": "전체 전환율(%)"},
            "3개월 전환율(%)": {"매장": s, "항목": "3개월 전환율(%)"},
            "방문빈도": {"매장": s, "항목": "방문빈도"},
            "이탈율(%)": {"매장": s, "항목": "이탈율(%)"},
            "유지기간": {"매장": s, "항목": "유지기간"}
        }

        for m in all_months:
            m_df = s_data[s_data['연월'] == m]
            if m_df.empty:
                for k in metrics_rows.keys(): metrics_rows[k][m] = 0
                continue
            
            v_ids = m_df['카드번호'].unique()
            total_v = len(v_ids)
            new_v_ids = m_df[m_df['first_v'].dt.strftime('%Y-%m') == m]['카드번호'].unique()
            new_v = len(new_v_ids)
            
            if new_v > 0:
                new_cust_full = s_data[s_data['카드번호'].isin(new_v_ids)].groupby('카드번호').first()
                o_conv = round(len(new_cust_full[new_cust_full['total_v_all'] >= 2]) / new_v * 100, 1)
                c_3m = round(len(new_cust_full[(new_cust_full['second_date'].notnull()) & (new_cust_full['second_date'] <= new_cust_full['first_v'] + timedelta(days=90))]) / new_v * 100, 1)
            else: o_conv = c_3m = 0.0

            v_stats = s_data[s_data['카드번호'].isin(v_ids)].groupby('카드번호').first()
            loyal_v = len(v_stats[v_stats['total_v_all'] >= 4])
            poten_v = len(v_stats[(v_stats['total_v_all'] >= 2) & (v_stats['total_v_all'] <= 3)])
            ret_pool = v_stats[v_stats['total_v_all'] >= 2]

            metrics_rows["매출액"][m] = int(m_df['net_sales'].sum())
            metrics_rows["전체 방문자수"][m] = total_v
            metrics_rows["신규 방문자수"][m] = new_v
            metrics_rows["신규비율(%)"][m] = round(new_v/total_v*100, 1) if total_v > 0 else 0
            metrics_rows["재방문자수"][m] = total_v - new_v
            metrics_rows["재방문자비율(%)"][m] = round((total_v-new_v)/total_v*100, 1) if total_v > 0 else 0
            metrics_rows["잠재 단골(2~3회)"][m] = poten_v
            metrics_rows["충성고객(4회이상)"][m] = loyal_v
            metrics_rows["충성고객비율(%)"][m] = round(loyal_v/total_v*100, 1) if total_v > 0 else 0
            metrics_rows["전체 전환율(%)"][m] = o_conv
            metrics_rows["3개월 전환율(%)"][m] = c_3m
            metrics_rows["방문빈도"][m] = round(ret_pool['total_v_all'].mean(), 1) if not ret_pool.empty else 1.0
            metrics_rows["이탈율(%)"][m] = round(len(ret_pool[ret_pool['last_v'] <= data_end_date - timedelta(days=90)]) / len(ret_pool) * 100, 1) if not ret_pool.empty else 0
            metrics_rows["유지기간"][m] = round((ret_pool['last_v'] - ret_pool['first_v']).dt.days.mean(), 1) if not ret_pool.empty else 0

        for row in metrics_rows.values():
            all_store_metrics.append(row)

    final_df = pd.DataFrame(all_store_metrics)
    st.subheader("📊 지점별 월간 통합 분석 데이터")
    st.dataframe(final_df, use_container_width=True)

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        final_df.to_excel(writer, index=False, sheet_name='Sheet1')
    
    st.download_button(
        label="📂 보정된 통합 엑셀 다운로드",
        data=buffer.getvalue(),
        file_name=f"모모유부_보정데이터_{datetime.now().strftime('%Y%m%d')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
else:

    st.error(f"오류: {status}")
