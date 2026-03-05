import streamlit as st
import pandas as pd
import re
import io
import os
from datetime import datetime

# ==========================================
# CONSTANTS & MAPS
# ==========================================
device_map_all = {
    # DG Active Power
    "DG1 active power": "BAG011UP01PV",
    "DG2 active power": "BAG021UP01PV",
    "DG3 active power": "BAG031UP01PV",
    "DG4 active power": "BAG041UP01PV",
    "DG5 active power": "BAG051UP01PV",
    "DG6 active power": "BAG061UP01PV",

    # DG CA Temperature
    "DG1 CA Temp": "SNB011T004PV",
    "DG2 CA Temp": "SNB021T004PV",
    "DG3 CA Temp": "SNB031T004PV",
    "DG4 CA Temp": "SCA041TE601PV",
    "DG5 CA Temp": "SCA051TE601PV",
    "DG6 CA Temp": "SCA061TE601PV",

    # Feeders and Plant Load
    "Feeder 1": "BAO901UP01PV",
    "Feeder 2": "BAO902UP01PV",
    "Feeder 3": "BAO903UP01PV",
    "Feeder 4": "BAO904UP01PV",
    "Total plant load": "BAO900UP01AV",
    "Ideal load": "SAB901UP01PV",

    # Line 1
    'Kiln 1': '891MV001Q07J01',
    'RM1 FAN': '321FN400M01Q01',
    'RM1 DRIVE': '321MD140M01J01',
    'CM1': '531MD140M01Q01',
    'CM2': '532MD140M01Q01',
    'RM2 FAN': '226FAD5DH10AV',

    #Line 2
    'RM2 DRIVE': '226DR83DH10AV',
    'UPSTREAM KILN': '321DH03DH10AV',
    'DOWNSTREAM KILN': '326DH03DH10AV',
    'CM3 FAN': '436FAE5DH10AV',
    'CM3 DRIVE': '436DRA9DH10UV',
    'CM3 DRIVE 2': '436DRA9DH20AV'
}

# ==========================================
# LOGIC 1: MASTER PROCESS (GENSET)
# ==========================================
def master_process(groups_dict):
    def extract_group_data(df, group_type):
        if len(df) < 24:
            return None
        
        if group_type == "Dg1_2":
            if df.shape[1] > 6:
                subset = df.iloc[0:24, [0, 1, 6]].copy()
                subset.columns = ['Hour', 'DG1 Kwh', 'DG2 Kwh']
            else:
                return None
                
        elif group_type == "dg3":
            subset = df.iloc[0:24, [0, 1]].copy()
            subset.columns = ['Hour', 'DG3 Kwh']
            
        elif group_type in ["g4", "g5", "g6"]:
            subset = df.iloc[0:24, [0, 1]].copy()
            num = group_type[1] 
            subset.columns = ['Hour', f'DG{num} Mwh']
            
        else:
            return None

        if 'Date' in df.columns:
            subset.insert(0, 'Date', df['Date'].iloc[0])
        else:
            return None

        subset['Hour'] = subset['Hour'].astype(str).str.strip()
        
        # Convert all to Megawatts
        kwh_cols = [col for col in subset.columns if 'Kwh' in col]
        subset[kwh_cols] = subset[kwh_cols].apply(pd.to_numeric, errors='coerce') / 1000
        subset.columns = [col.replace('Kwh', 'Mwh') for col in subset.columns]

        return subset

    all_processed_groups = []

    for group_type, df_list in groups_dict.items():
        processed_dfs = []
        for raw_df in df_list:
            extracted = extract_group_data(raw_df, group_type)
            if extracted is not None:
                processed_dfs.append(extracted)

        if processed_dfs:
            group_combined = pd.concat(processed_dfs, ignore_index=True)
            all_processed_groups.append(group_combined)

    if not all_processed_groups:
        return pd.DataFrame()

    final_df = all_processed_groups[0]
    for next_group in all_processed_groups[1:]:
        final_df = pd.merge(final_df, next_group, on=['Date', 'Hour'], how='outer')

    if not final_df.empty:
        final_df['Hour'] = final_df['Hour'].astype(str)
        final_df['Date'] = pd.to_datetime(final_df['Date'])
        final_df = final_df.sort_values(['Date', 'Hour']).reset_index(drop=True)
        final_df['Date'] = final_df['Date'].dt.strftime('%d/%m/%Y')
        
    return final_df

# ==========================================
# LOGIC 2: START/STOP DEVICE PROCESSING
# ==========================================
def process_device_data(df, csv_file, device_map):
    df.rename(columns={'$Date': 'Date', '$Time': 'Time'}, inplace=True)
    cols = [col for col in df.columns if col not in ['Date', 'Time']]
    last_valid_row = df.dropna().index[-1]
    df_cleaned = df.loc[:last_valid_row]
    df1 = df_cleaned.copy()
    
    for col in cols:
        df1[col] = df1[col].astype(float)
        df1[f'{col}_running_avg'] = df1[col].rolling(window=3, center=True, min_periods=1).mean()

    df1['Hour'] = pd.to_datetime(df1['Time'], format='%H:%M:%S').dt.hour
    df1['Time'] = pd.to_datetime(df1['Time'], format='%H:%M:%S').dt.time

    def stop(df, c, thresh=1):
        col = f'{c}_running_avg'
        cond1 = df[col].shift(1) > thresh
        cond2 = (df[col] + df[col].shift(-1)) == 0
        stops = (cond1 & cond2).astype(int)
        if len(df) >= 2:
            if (df[col].iloc[-2] > 1000) and (df[col].iloc[-1] == 0):
                stops.iloc[-1] = 1
        return stops

    def start(df, c, thresh=1):
        col = f'{c}_running_avg'
        cond1 = (df[col].shift(1) + df[col].shift(2)) == 0
        cond2 = df[col] > thresh
        starts = (cond1 & cond2).astype(int)
        if df[col].iloc[0] > thresh:
            starts.iloc[0] = 0
        if df[col].iloc[0] == 0 and df[col].iloc[1] > thresh:
            starts.iloc[1] = 1
        return starts

    for col in cols:
        df1[f'{col}_stops'] = stop(df1, col)
        df1[f'{col}_starts'] = start(df1, col)

    mapped_device = {v: k for k, v in device_map.items()}
    arranged_cols = [f'{col}_{a}' for col in cols for a in ['starts','stops']]
    renamed_cols = [f'{mapped_device.get(col, col)} {a[:-1].upper()}' for col in cols for a in ['starts','stops']]

    hourly_sum_df = df1.groupby(['Date', 'Hour'])[arranged_cols].sum().reset_index()
    rename_map = dict(zip(arranged_cols, renamed_cols))
    hourly_sum_df.rename(columns=rename_map, inplace=True)

    save_path_list = os.path.split(csv_file)[-1].split(' ')
    if len(save_path_list) > 4:
        save_path_list[4] = renamed_cols[0] + '....' + renamed_cols[-1]
    else:
        save_path_list.append(renamed_cols[0] + '....' + renamed_cols[-1])
        
    save_path_name = ' '.join(save_path_list[:-1])
    return hourly_sum_df, save_path_name 

# ==========================================
# LOGIC 3: CA TEMPERATURE PROCESSING
# ==========================================
def process_CA_temp(df, csv_file, device_map):
    df.rename(columns={'$Date': 'Date', '$Time': 'Time'}, inplace=True)
    cols = [col for col in df.columns if col not in ['Date', 'Time']]
    last_valid_row = df.dropna().index[-1]
    df_cleaned = df.loc[:last_valid_row]
    df1 = df_cleaned.copy()
    
    df1['Hour'] = pd.to_datetime(df1['Time'], format='%H:%M:%S').dt.hour
    df1['Time'] = pd.to_datetime(df1['Time'], format='%H:%M:%S').dt.time
    df1['Datetime'] = pd.to_datetime(df1['Date'].astype(str) + ' ' + df1['Time'].astype(str), format='%m/%d/%y %H:%M:%S')
    
    mapped_device = {v: k for k, v in device_map.items()}
    arranged_cols = [f'{col}' for col in cols]
    renamed_cols = [f'{mapped_device.get(col, col)}' for col in cols]

    hourly_sum_df = df1.groupby(['Date', 'Hour'])[arranged_cols].mean().reset_index()
    rename_map = dict(zip(arranged_cols, renamed_cols))
    hourly_sum_df.rename(columns=rename_map, inplace=True)
    
    for col in renamed_cols:
        hourly_sum_df[col] = hourly_sum_df[col].round(1)

    save_path_list = os.path.split(csv_file)[-1].split(' ')
    if len(save_path_list) > 4:
        save_path_list[4] = renamed_cols[0] + '....' + renamed_cols[-1]
    else:
        save_path_list.append(renamed_cols[0] + '....' + renamed_cols[-1])
        
    save_path_name = ' '.join(save_path_list)
    return hourly_sum_df, save_path_name

# ==========================================
# LOGIC 4: POWER/ENERGY PROCESSING
# ==========================================
def process_power(df, csv_file, device_map, divide_by_1000=False):
    df.rename(columns={'$Date': 'Date', '$Time': 'Time'}, inplace=True)
    cols = [col for col in df.columns if col not in ['Date', 'Time']]
    last_valid_row = df.dropna().index[-1]
    df_cleaned = df.loc[:last_valid_row]
    df1 = df_cleaned.copy()
    
    df1['Hour'] = pd.to_datetime(df1['Time'], format='%H:%M:%S').dt.hour
    df1['Time'] = pd.to_datetime(df1['Time'], format='%H:%M:%S').dt.time
    df1['Datetime'] = pd.to_datetime(df1['Date'].astype(str) + ' ' + df1['Time'].astype(str), format='%m/%d/%y %H:%M:%S')
    
    df1['time_diff_sec'] = df1['Datetime'].diff().dt.total_seconds().fillna(0)

    for col in cols:
        df1[f'{col}_Energy'] = ((df1[f'{col}'] + df1[f'{col}'].shift(1).fillna(0)) / 2 * df1['time_diff_sec'] / 3600)
        
    mapped_device = {v: k for k, v in device_map.items()}
    arranged_cols = [f'{col}_{a}' for col in cols for a in ['Energy']]
    renamed_cols = [f'{mapped_device.get(col, col)} {a}' for col in cols for a in ['Energy']]

    hourly_sum_df = df1.groupby(['Date', 'Hour'])[arranged_cols].sum().reset_index()
    
    if divide_by_1000:
        hourly_sum_df[arranged_cols] = hourly_sum_df[arranged_cols] / 1000

    rename_map = dict(zip(arranged_cols, renamed_cols))
    hourly_sum_df.rename(columns=rename_map, inplace=True)
    
    for col in renamed_cols:
        hourly_sum_df[col] = hourly_sum_df[col].round(2)

    save_path_list = os.path.split(csv_file)[-1].split(' ')
    if len(save_path_list) > 4:
        save_path_list[4] = renamed_cols[0] + '....' + renamed_cols[-1]
    else:
        save_path_list.append(renamed_cols[0] + '....' + renamed_cols[-1])
        
    save_path_name = ' '.join(save_path_list)
    return hourly_sum_df, save_path_name


# ==========================================
# STREAMLIT UI HELPER
# ==========================================
def create_excel_download(df, base_filename, key_prefix):
    # Strip any trailing .csv and ensure .xlsx extension
    clean_name = base_filename.replace('.csv', '').strip() + '.xlsx'
    
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Data')
    
    st.download_button(
        label=f"📥 Download {clean_name}",
        data=buffer.getvalue(),
        file_name=clean_name,
        mime="application/vnd.ms-excel",
        key=f"{key_prefix}_{clean_name}"
    )


# ==========================================
# STREAMLIT APP LAYOUT
# ==========================================
st.set_page_config(page_title="Plant Reporting Tools", layout="wide")

st.title("Plant Reporting Dashboard")

# Create tabs for all functionalities
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "⚡ Genset", 
    "🔄 Start/Stop", 
    "🌡️ CA Temp", 
    "⚡ Active Energy", 
    "🔌 Feeder Unit"
])

# ------------------------------------------
# TAB 1: GENSET CONSOLIDATOR
# ------------------------------------------
with tab1:
    st.markdown("Upload daily **mfamosing** files to merge them based on the genset group.")
    uploaded_files = st.file_uploader("Upload Genset files", accept_multiple_files=True, type=['xls', 'xlsx', 'csv'], key="genset_uploader")

    if uploaded_files:
        if st.button("Generate Genset Report"):
            with st.spinner("Processing Genset files..."):
                file_data_list = []
                for uploaded_file in uploaded_files:
                    fname = uploaded_file.name
                    match = re.search(r'(\d{8})', fname)
                    if match:
                        try:
                            d_obj = datetime.strptime(match.group(1), '%m%d%Y').date()
                            file_data_list.append((d_obj, uploaded_file))
                        except ValueError:
                            pass

                file_data_list.sort(key=lambda x: x[0])
                input_data = {"Dg1_2": [], "dg3": [], "g4": [], "g5": [], "g6": []}
                files_processed_count = 0

                for date_val, file_obj in file_data_list:
                    f_low = file_obj.name.lower()
                    if not f_low.startswith('mfamosing'): continue
                    
                    group_key = None
                    if "genset 1-2" in f_low: group_key = "Dg1_2"
                    elif "genset 3" in f_low: group_key = "dg3"
                    elif "g4 tot" in f_low: group_key = "g4"
                    elif "g5 tot" in f_low: group_key = "g5"
                    elif "g6 tot" in f_low: group_key = "g6"
                    
                    if group_key:
                        file_obj.seek(0)
                        df = pd.read_csv(file_obj, encoding='latin1') if file_obj.name.endswith('.csv') else pd.read_excel(file_obj)
                        df['Date'] = date_val
                        input_data[group_key].append(df)
                        files_processed_count += 1

                if files_processed_count > 0:
                    final_report = master_process(input_data)
                    if not final_report.empty:
                        st.success("Consolidation complete!")
                        st.dataframe(final_report, hide_index=True, height=600)
                        
                        buffer = io.BytesIO()
                        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                            final_report.to_excel(writer, index=False, sheet_name='Consolidated')
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            st.download_button(label="Download Excel Report", data=buffer.getvalue(), file_name="consolidated_genset_report.xlsx", mime="application/vnd.ms-excel")
                        with col2:
                            st.write("Or copy data directly:")
                            df_clipboard = final_report.copy()
                            df_clipboard['Hour'] = "'" + df_clipboard['Hour'].astype(str)
                            st.code(df_clipboard.to_csv(index=False, header=False, sep='\t'), language="text")
                    else:
                        st.warning("Resulting report was empty.")
                else:
                    st.error("No valid files found.")

# ------------------------------------------
# TAB 2: START/STOP PROCESSOR
# ------------------------------------------
with tab2:
    st.markdown("Upload raw CSV files to process the **Start/Stop load profile**.")
    ss_files = st.file_uploader("Upload Start/Stop CSV files", accept_multiple_files=True, type=['csv'], key="ss_uploader")
    if ss_files and st.button("Process Start/Stop Files"):
        with st.spinner("Processing load profiles..."):
            st.success("Files processed successfully!")
            for file_obj in ss_files:
                try:
                    df_raw = pd.read_csv(file_obj)
                    processed_df, out_filename = process_device_data(df_raw, file_obj.name, device_map_all)
                    create_excel_download(processed_df, out_filename, "ss")
                except Exception as e:
                    st.error(f"Error on {file_obj.name}: {e}")

# ------------------------------------------
# TAB 3: CA TEMPERATURE PROCESSOR
# ------------------------------------------
with tab3:
    st.markdown("Upload raw CSV files to process the **Charged Air (CA) Temperature**.")
    ca_files = st.file_uploader("Upload CA Temp CSV files", accept_multiple_files=True, type=['csv'], key="ca_uploader")
    if ca_files and st.button("Process CA Temp Files"):
        with st.spinner("Processing CA Temperatures..."):
            st.success("Files processed successfully!")
            for file_obj in ca_files:
                try:
                    df_raw = pd.read_csv(file_obj)
                    processed_df, out_filename = process_CA_temp(df_raw, file_obj.name, device_map_all)
                    create_excel_download(processed_df, out_filename, "ca")
                except Exception as e:
                    st.error(f"Error on {file_obj.name}: {e}")

# ------------------------------------------
# TAB 4: ACTIVE ENERGY PROCESSOR
# ------------------------------------------
with tab4:
    st.markdown("Upload raw CSV files to process **Active Energy**.")
    ae_divide = st.checkbox("Divide final answer by 1000?", value=False)
    ae_files = st.file_uploader("Upload Active Energy CSV files", accept_multiple_files=True, type=['csv'], key="ae_uploader")
    if ae_files and st.button("Process Active Energy Files"):
        with st.spinner("Processing Active Energy..."):
            st.success("Files processed successfully!")
            for file_obj in ae_files:
                try:
                    df_raw = pd.read_csv(file_obj)
                    processed_df, out_filename = process_power(df_raw, file_obj.name, device_map_all, divide_by_1000=ae_divide)
                    create_excel_download(processed_df, out_filename, "ae")
                except Exception as e:
                    st.error(f"Error on {file_obj.name}: {e}")

# ------------------------------------------
# TAB 5: FEEDER UNIT PROCESSOR
# ------------------------------------------
with tab5:
    st.markdown("Upload raw CSV files to process the **Feeder Unit**.")
    fu_files = st.file_uploader("Upload Feeder Unit CSV files", accept_multiple_files=True, type=['csv'], key="fu_uploader")
    if fu_files and st.button("Process Feeder Unit Files"):
        with st.spinner("Processing Feeder Units..."):
            st.success("Files processed successfully!")
            for file_obj in fu_files:
                try:
                    df_raw = pd.read_csv(file_obj)
                    processed_df, out_filename = process_power(df_raw, file_obj.name, device_map_all, divide_by_1000=False)
                    create_excel_download(processed_df, out_filename, "fu")
                except Exception as e:
                    st.error(f"Error on {file_obj.name}: {e}")
