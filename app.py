import streamlit as st
import pandas as pd
import re
import io
from datetime import datetime

# ==========================================
# CONSTANTS & MAPS
# ==========================================
device_map_all = {
    'Kiln 1': '891MV001Q07J01',
    'RM1 FAN': '321FN400M01Q01',
    'RM1 DRIVE': '321MD140M01J01',
    'CM1': '531MD140M01Q01',
    'CM2': '532MD140M01Q01',
    'RM2 FAN': '226FAD5DH10AV',
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
    """
    Larger function: Receives a dictionary {group_name: [list of dfs]}.
    Returns a consolidated DataFrame with all metrics joined by Date and Hour.
    """
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
    # [Added] Rename $Date and $Time to Date and Time to prevent KeyErrors later
    df.rename(columns={'$Date': 'Date', '$Time': 'Time'}, inplace=True)
    
    # Identify device columns (ignoring Date and Time)
    cols = [col for col in df.columns if col not in ['Date', 'Time']]
    
    # Find the index of the very last row that doesn't have any NaNs
    last_valid_row = df.dropna().index[-1]

    # Slice the dataframe to keep everything from the top down to that last valid row
    df_cleaned = df.loc[:last_valid_row]
    
    # Create a new column for the running average
    df1 = df_cleaned.copy()
    
    for col in cols:
        df1[col] = df1[col].astype(float)
        df1[f'{col}_running_avg'] = df1[col].rolling(window=3, center=True, min_periods=1).mean()

    # [Changed] Extract Hour BEFORE converting to dt.time
    # 1. Grab the hour directly from the datetime conversion
    df1['Hour'] = pd.to_datetime(df1['Time'], format='%H:%M:%S').dt.hour
    
    # 2. Then run your original code to convert Time to time objects
    df1['Time'] = pd.to_datetime(df1['Time'], format='%H:%M:%S').dt.time

    # Embedded stop function
    def stop(df, c, thresh=1):
        col = f'{c}_running_avg'
        
        # --- 1. VECTORIZED LOGIC ---
        cond1 = df[col].shift(1) > thresh
        cond2 = (df[col] + df[col].shift(-1)) == 0
        stops = (cond1 & cond2).astype(int)
        
        # --- 2. EDGE CASE PATCH ---
        if len(df) >= 2:
            if (df[col].iloc[-2] > 1000) and (df[col].iloc[-1] == 0):
                stops.iloc[-1] = 1

        return stops

    # Embedded start function
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

    # Apply functions to each column
    for col in cols:
        df1[f'{col}_stops'] = stop(df1, col)
        df1[f'{col}_starts'] = start(df1, col)

    # Invert the device map safely
    mapped_device = {v: k for k, v in device_map.items()}

    # Setup column arrangements and renames
    arranged_cols = [f'{col}_{a}' for col in cols for a in ['starts','stops']]
    renamed_cols = [f'{mapped_device.get(col, col)} {a[:-1].upper()}' for col in cols for a in ['starts','stops']]

    # Group by Date and Hour, select only the columns in arranged_cols, and sum them
    hourly_sum_df = df1.groupby(['Date', 'Hour'])[arranged_cols].sum().reset_index()

    # 1. Zip the two lists together to create a mapping dictionary 
    rename_map = dict(zip(arranged_cols, renamed_cols))

    # 2. Apply the mapping to the dataframe
    hourly_sum_df.rename(columns=rename_map, inplace=True)

    # 3. Construct File Name 
    save_path_list = os.path.split(csv_file)[-1].split(' ')
    
    # [Added] Safety check for filename splitting
    if len(save_path_list) > 4:
        save_path_list[4] = renamed_cols[0] + '....' + renamed_cols[-1]
    else:
        save_path_list.append(renamed_cols[0] + '....' + renamed_cols[-1])
        
    save_path_name = ' '.join(save_path_list)
    
    # Note: df.to_csv is purposely omitted here to prevent saving to the server's disk

    return hourly_sum_df, save_path_name 


# ==========================================
# STREAMLIT UI
# ==========================================
st.set_page_config(page_title="Plant Reporting Tools", layout="wide")

st.title("Plant Reporting Dashboard")

# Create tabs for the two different functionalities
tab1, tab2 = st.tabs(["⚡ Genset Consolidator", "🔄 Start/Stop Profile Processor"])

# ------------------------------------------
# TAB 1: GENSET CONSOLIDATOR
# ------------------------------------------
with tab1:
    st.markdown("Upload your daily **mfamosing** files (Excel or CSV). The app will merge them based on the genset group.")
    
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
                            st.warning(f"Could not parse date in filename: {fname}")
                    else:
                        st.warning(f"No 8-digit date found in filename: {fname}")

                file_data_list.sort(key=lambda x: x[0])
                input_data = {"Dg1_2": [], "dg3": [], "g4": [], "g5": [], "g6": []}
                files_processed_count = 0

                for date_val, file_obj in file_data_list:
                    f_low = file_obj.name.lower()
                    if not f_low.startswith('mfamosing'):
                        continue

                    group_key = None
                    if "genset 1-2" in f_low: group_key = "Dg1_2"
                    elif "genset 3" in f_low: group_key = "dg3"
                    elif "g4 tot" in f_low: group_key = "g4"
                    elif "g5 tot" in f_low: group_key = "g5"
                    elif "g6 tot" in f_low: group_key = "g6"
                    
                    if group_key:
                        try:
                            file_obj.seek(0)
                            if file_obj.name.endswith('.csv'):
                                df = pd.read_csv(file_obj, encoding='latin1')
                            else:
                                df = pd.read_excel(file_obj)
                                
                            df['Date'] = date_val
                            input_data[group_key].append(df)
                            files_processed_count += 1
                        except Exception as e:
                            st.error(f"Error reading {file_obj.name}: {e}")

                if files_processed_count > 0:
                    final_report = master_process(input_data)
                    
                    if not final_report.empty:
                        st.success("Consolidation complete!")
                        st.dataframe(final_report, hide_index=True, height=900)
                        
                        buffer = io.BytesIO()
                        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                            final_report.to_excel(writer, index=False, sheet_name='Consolidated')
                            
                        buffer.seek(0)
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.download_button(
                                label="Download Excel Report",
                                data=buffer,
                                file_name="consolidated_genset_report.xlsx",
                                mime="application/vnd.ms-excel"
                            )
                            
                        with col2:
                            st.write("Or copy data directly (click icon in top right):")
                            df_clipboard = final_report.copy()
                            df_clipboard['Hour'] = "'" + df_clipboard['Hour'].astype(str)
                            tsv_data = df_clipboard.to_csv(index=False, header=False, sep='\t')
                            st.code(tsv_data, language="text")
                    else:
                        st.warning("Processed files but the resulting report was empty.")
                else:
                    st.error("No valid files matching the criteria were found.")

# ------------------------------------------
# TAB 2: START/STOP PROCESSOR
# ------------------------------------------
with tab2:
    st.markdown("Upload your raw CSV files to process the **Start/Stop load profile**.")
    
    ss_files = st.file_uploader("Upload Start/Stop CSV files", accept_multiple_files=True, type=['csv'], key="ss_uploader")
    
    if ss_files:
        if st.button("Process Start/Stop Files"):
            with st.spinner("Processing load profiles..."):
                
                # Display a success message holder
                st.success("Files processed successfully! Download below:")
                
                # Process each file individually
                for file_obj in ss_files:
                    try:
                        # Read the raw data
                        df_raw = pd.read_csv(file_obj)
                        
                        # Process using the mapped function
                        processed_df, out_filename = process_device_data(df_raw, file_obj.name, device_map_all)
                        
                        # Ensure filename ends with .csv
                        if not out_filename.endswith('.csv'):
                            out_filename = out_filename + '.csv'
                            
                        # Convert to CSV strictly in-memory
                        csv_bytes = processed_df.to_csv(index=False).encode('utf-8')
                        
                        # Provide a download button for this specific file
                        st.download_button(
                            label=f"📥 Download {out_filename}",
                            data=csv_bytes,
                            file_name=out_filename,
                            mime="text/csv",
                            key=file_obj.name # Unique key required for multiple buttons
                        )
                    except Exception as e:
                        st.error(f"Failed to process {file_obj.name}. Error: {e}")
