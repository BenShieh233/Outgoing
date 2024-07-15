import os
import pandas as pd
import numpy as np
from datetime import datetime

def get_tables(file_path, sheet_names):
    df = pd.concat([pd.read_excel(file_path, sheet_name=sheet_name) for sheet_name in sheet_names[2:]], axis=0)
    df = df[['SKU', 'Wayfair SKU', 'Qty', 'PROBLEM NUMBER']]
    df['SKU'] = df['SKU'].str.strip()
    df['Wayfair SKU'] = df['Wayfair SKU'].str.strip()
    df['Problem Number'] = df['PROBLEM NUMBER'].apply(lambda x: str(int(x)) if isinstance(x, (float, int)) and not np.isnan(x) else x).fillna("0")
    
    df = df[['SKU', 'Wayfair SKU', 'Qty', 'Problem Number']]

    Qty = df[['SKU', 'Wayfair SKU', 'Qty']]
    PN = df[['SKU', 'Wayfair SKU', 'Qty', 'Problem Number']]

    return Qty, PN

def cal_qty(df):
    summary = df.groupby(['SKU', 'Wayfair SKU']).agg(total_qty=('Qty', 'sum')).reset_index()
    return summary

def cal_PN(df):
    df = df.assign(Problem_Number=df['Problem Number'].str.split('-')).explode('Problem_Number')
    df_PN = df[['SKU', 'Wayfair SKU', 'Qty', 'Problem_Number']]

    grouped = df_PN.groupby(['SKU', 'Wayfair SKU', 'Problem_Number']).agg(total_qty=('Qty', 'sum')).reset_index()
    pivot_table = grouped.pivot_table(index=['SKU', 'Wayfair SKU'], columns='Problem_Number', values='total_qty', fill_value=0).reset_index()
    pivot_table.columns = ['SKU', 'Wayfair SKU'] + [f'PN-{col}' for col in pivot_table.columns[2:]]
    pivot_table = pivot_table.reindex(sorted(pivot_table.columns[2:], key=lambda x: int(x.split('-')[1])), axis=1)

    return pivot_table

def merge_tables(Qty, PN):
    final_summary = pd.concat([cal_qty(Qty), cal_PN(PN)], axis = 1)
    return final_summary

if __name__ == "__main__":

    file_name = "July_24 CARRO USA OUTGOING.xlsx"
    current_dir = os.getcwd()
    file_path = os.path.join(current_dir, file_name)

    xls = pd.ExcelFile(file_path)

    sheet_names = xls.sheet_names
    
    Qty, PN = get_tables(file_path, sheet_names)[0], get_tables(file_path, sheet_names)[1]

    final_summary = merge_tables(Qty, PN)

    # Generate timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Construct filename with timestamp
    saved_file_name = f"Summary_{timestamp}.xlsx"

    final_summary.to_excel(saved_file_name)

    print(f"Data saved to '{file_name}'")

