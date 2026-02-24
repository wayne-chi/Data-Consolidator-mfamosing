import streamlit as st
import pandas as pd
import re
import io
from datetime import datetime

# --- Logic: Master Process Function ---
def master_process(groups_dict):
    """
    Larger function: Receives a dictionary {group_name: [list of dfs]}.
    Returns a consolidated DataFrame with all metrics joined by Date and Hour.
    """

    def extract_group_data(df, group_type):
        """
        Nested helper: Slices rows 2-25 and renames specific columns based on group.
        """
        # Ensure we don't crash if the file is shorter than expected
        if len(df) < 24:
            return None

        # Slicing rows 2 to 25 (pandas indices 0 to 23 of the data area)
        # Note: Depending on headers, pandas might read the header as row 0. 
        # Assuming your slicing logic works for your specific file structure:
        
        if group_type == "Dg1_2":
            # Columns A (Hour), B (DG1), G (DG2) -> Indices 0, 1, 6
            # Check if columns exist to avoid index errors
            if df.shape[1] > 6:
                subset = df.iloc[0:24, [0, 1, 6]].copy()
                subset.columns = ['Hour', 'DG1 Kwh', 'DG2 Kwh']
            else:
                return None
                
        elif group_type == "dg3":
            # Columns A, B -> Indices 0, 1
            subset = df.iloc[0:24, [0, 1]].copy()
            subset.columns = ['Hour', 'DG3 Kwh']
            
        elif group_type in ["g4", "g5", "g6"]:
            # Columns A, B -> Indices 0, 1
            subset = df.iloc[0:24, [0, 1]].copy()
            num = group_type[1] # Extracts digit from 'g4', 'g5', or 'g6'
            subset.columns = ['Hour', f'DG{num} Mwh']
            
        else:
            return None

        # Ensure 'Date' is pulled from the parent DataFrame attached during reading
        if 'Date' in df.columns:
            subset.insert(0, 'Date', df['Date'].iloc[0])
        else:
            # Fallback if date wasn't attached correctly
            return None

        # Standardize Hour strings for clean merging (e.g., "00-01")
        subset['Hour'] = subset['Hour'].astype(str).str.strip()
        # convert all to Megawatts
        kwh_cols = [col for col in subset.columns if 'Kwh' in col]
        subset[kwh_cols] = subset[kwh_cols].apply(pd.to_numeric, errors='coerce') / 1000
        subset.columns = [col.replace('Kwh', 'Mwh') for col in subset.columns]

        return subset

    all_processed_groups = []

    # Process categories: Dg1_2, dg3, g4, g5, g6
    for group_type, df_list in groups_dict.items():
        processed_dfs = []
        for raw_df in df_list:
            extracted = extract_group_data(raw_df, group_type)
            if extracted is not None:
                processed_dfs.append(extracted)

        if processed_dfs:
            # Append multiple dates for this group
            group_combined = pd.concat(processed_dfs, ignore_index=True)
            all_processed_groups.append(group_combined)

    if not all_processed_groups:
        return pd.DataFrame()

    # Consolidate all groups into one table using an outer join on Date and Hour
    final_df = all_processed_groups[0]
    for next_group in all_processed_groups[1:]:
        final_df = pd.merge(final_df, next_group, on=['Date', 'Hour'], how='outer')

    # Sort chronologically
    if not final_df.empty:
        final_df['Hour'] = final_df['Hour'].astype(str)
        # Ensure Date is datetime for sorting
        final_df['Date'] = pd.to_datetime(final_df['Date'])
        
        # Sort values first
        final_df = final_df.sort_values(['Date', 'Hour']).reset_index(drop=True)
        
        # Apply the requested string format
        final_df['Date'] = final_df['Date'].dt.strftime('%d/%m/%Y')
        
    return final_df

# --- Streamlit UI ---
st.set_page_config(page_title="Genset Report Generator", layout="wide")

st.title("Genset Report Consolidator")
st.markdown("""
Upload your daily **mfamosing** files (Excel or CSV). 
The app will merge them based on the genset group and generate a consolidated Excel report.
""")

# File Uploader
uploaded_files = st.file_uploader("Upload files", accept_multiple_files=True, type=['xls', 'xlsx', 'csv'])

if uploaded_files:
    if st.button("Generate Report"):
        with st.spinner("Processing files..."):
            
            # 1. Pre-sort files by date logic (extracted from filename)
            # This ensures they are processed in order
            file_data_list = []
            
            for uploaded_file in uploaded_files:
                fname = uploaded_file.name
                
                # Regex to find 8 digit date (MMDDYYYY)
                match = re.search(r'(\d{8})', fname)
                
                if match:
                    try:
                        d_obj = datetime.strptime(match.group(1), '%m%d%Y').date()
                        file_data_list.append((d_obj, uploaded_file))
                    except ValueError:
                        st.warning(f"Could not parse date in filename: {fname}")
                else:
                    st.warning(f"No 8-digit date found in filename: {fname}")

            # Sort by date object
            file_data_list.sort(key=lambda x: x[0])

            # 2. Organize into groups
            input_data = {"Dg1_2": [], "dg3": [], "g4": [], "g5": [], "g6": []}
            
            files_processed_count = 0

            for date_val, file_obj in file_data_list:
                f_low = file_obj.name.lower()
                
                # Check if it is an 'mfamosing' file (optional filter)
                if not f_low.startswith('mfamosing'):
                    continue

                # Map filenames to keys
                group_key = None
                if "genset 1-2" in f_low: group_key = "Dg1_2"
                elif "genset 3" in f_low: group_key = "dg3"
                elif "g4 tot" in f_low: group_key = "g4"
                elif "g5 tot" in f_low: group_key = "g5"
                elif "g6 tot" in f_low: group_key = "g6"
                
                if group_key:
                    try:
                        # Reset file pointer to beginning before reading
                        file_obj.seek(0)
                        
                        if file_obj.name.endswith('.csv'):
                            df = pd.read_csv(file_obj, encoding='latin1')
                        else:
                            df = pd.read_excel(file_obj)
                            
                        # Attach date
                        df['Date'] = date_val
                        input_data[group_key].append(df)
                        files_processed_count += 1
                        
                    except Exception as e:
                        st.error(f"Error reading {file_obj.name}: {e}")

            # 3. Run master process
            if files_processed_count > 0:
                final_report = master_process(input_data)
                
                if not final_report.empty:
                    st.success("Consolidation complete!")
                    # st.dataframe(final_report.head())
                    st.dataframe(final_report, hide_index=True, height=900)
                    # st.table(final_report.style.hide(axis="index"))
                    
                    # 4. Create Downloadable Excel
                    buffer = io.BytesIO()
                    # Use xlsxwriter for better compatibility with streams
                    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                        final_report.to_excel(writer, index=False, sheet_name='Consolidated')
                        
                    # Important: Seek to start of stream
                    buffer.seek(0)
                    # Create columns to put the download and copy options side-by-side
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
                        # Convert to tab-separated text so it drops the index and pastes neatly into Excel
                        tsv_data = final_report.to_csv(index=False, sep='\t')
                        st.code(tsv_data, language="text")
                    # st.download_button(
                    #     label="Download Excel Report",
                    #     data=buffer,
                    #     file_name="consolidated_genset_report.xlsx",
                    #     mime="application/vnd.ms-excel"
                    # )


                
                else:
                    st.warning("Processed files but the resulting report was empty. Check file content structure.")
            else:

                st.error("No valid files matching the criteria (mfamosing + genset keywords) were found.")





