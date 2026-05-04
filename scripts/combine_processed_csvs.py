import pandas as pd
import os
import glob

TARGET_COLUMNS = [
    'FEC_INGRESO', 'PROCEDENCIA', 'CDES_TIPOPROCESO', 'SALA_ORIGEN',
    'TIPO_DEMANDANTE', 'TIPO_DEMANDADO', 'SALA', 'FEC_VISTA',
    'MATERIA', 'SUB_MATERIA', 'ESPECÍFICA', 'PUB_PAGWEB',
    'PUB_PERUANO', 'FEC_DEVPJ', 'FEC_DEVPJ_1', 'DEPARTAMENTO',
    'PROVINCIA', 'DISTRITO'
]

COLUMN_MAPPING = {
    'ESPECIFICA': 'ESPECÍFICA',
}

PROCESSED_DIR = r'c:\Users\practicante.coe03\Desktop\Clases\Programacion Concurrente y Distribuida\semantic-search-court-records\datasets\processed'
OUTPUT_FILE = os.path.join(PROCESSED_DIR, 'processed_records.csv')

def combine_csvs():
    csv_files = glob.glob(os.path.join(PROCESSED_DIR, '*.csv'))
    csv_files = [f for f in csv_files if os.path.basename(f) != 'combined_processed_records.csv']
    
    print(f"Found {len(csv_files)} files to combine: {csv_files}")
    
    all_data = []
    
    for file_path in csv_files:
        print(f"Processing {file_path}...")
        try:
            header = pd.read_csv(file_path, nrows=0)
            existing_cols = header.columns.tolist()
            cols_to_read = []
            rename_map = {}
            
            for col in TARGET_COLUMNS:
                if col in existing_cols:
                    cols_to_read.append(col)
                elif col == 'ESPECÍFICA' and 'ESPECIFICA' in existing_cols:
                    cols_to_read.append('ESPECIFICA')
                    rename_map['ESPECIFICA'] = 'ESPECÍFICA'
            
            if not cols_to_read:
                print(f"Skipping {file_path}: No matching columns found.")
                continue

            chunk_iterator = pd.read_csv(file_path, usecols=cols_to_read, chunksize=100000, dtype=str)
            
            for chunk in chunk_iterator:
                if rename_map:
                    chunk = chunk.rename(columns=rename_map)
                
                for col in TARGET_COLUMNS:
                    if col not in chunk.columns:
                        chunk[col] = ""
                
                chunk = chunk[TARGET_COLUMNS]
                chunk = chunk.drop_duplicates()
                
                all_data.append(chunk)
                
        except Exception as e:
            print(f"Error processing {file_path}: {e}")

    if not all_data:
        print("No data found to combine.")
        return

    print("Merging data...")
    final_df = pd.concat(all_data, ignore_index=True)
    
    print("Removing global duplicates...")
    initial_count = len(final_df)
    final_df = final_df.drop_duplicates()
    final_count = len(final_df)
    
    print(f"Removed {initial_count - final_count} duplicate rows in total.")
    print(f"Saving {final_count} records to {OUTPUT_FILE}...")
    
    final_df.to_csv(OUTPUT_FILE, index=False, sep=',', encoding='utf-8')
    print(f"Success! Output saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    combine_csvs()