import pandas as pd
import numpy as np
from openpyxl import load_workbook


def get_tables(file_path, sheet_names):
    df = pd.concat([pd.read_excel(file_path, sheet_name=sheet_name) for sheet_name in sheet_names[2:]], axis=0)
    
    df = df[['SKU', 'Wayfair SKU', 'Qty', 'PROBLEM NUMBER']]
    df['Problem Number'] = df['PROBLEM NUMBER'].apply(lambda x: str(int(x)) if isinstance(x, (float, int)) and not np.isnan(x) else x)
    df = df[['SKU', 'Wayfair SKU', 'Qty', 'Problem Number']]

    Qty = df[['SKU', 'Wayfair SKU', 'Qty']]
    PN = df[['SKU', 'Wayfair SKU', 'Problem Number']]
    
    return Qty, PN

def cal_qty(df):
    summary = df.groupby(['SKU', 'Wayfair SKU']).agg(total_qty=('Qty', 'sum')).reset_index()
    return summary

def cal_PN(df):
    # Fill NaN values in 'Problem Number' with '0'
    df['Problem Number'] = df['Problem Number'].fillna('0')
    
    # Split 'Problem Number' by '-' and explode into separate rows
    df = df.assign(Problem_Number=df['Problem Number'].str.split('-')).explode('Problem_Number')
    
    # Convert 'Problem_Number' to integers
    df['Problem_Number'] = df['Problem_Number'].astype(int)
    
    # Group by 'SKU' and 'Wayfair SKU', and aggregate problem counts
    summary = df.groupby(['SKU', 'Wayfair SKU']).agg(
        problem_counts=('Problem_Number', lambda x: x.value_counts().to_dict())
    ).reset_index()
    
    # Convert problem counts dictionary into separate columns
    problem_df = summary['problem_counts'].apply(pd.Series).fillna(0).astype(int)
    problem_df.columns = [f'PN-{col}' for col in problem_df.columns]
    sorted_columns = sorted(problem_df.columns, key=lambda x: int(x.split('-')[1]))
    problem_df = problem_df.reindex(columns = sorted_columns)

    return problem_df

def merge_tables(Qty, PN):
    final_summary = pd.concat([cal_qty(Qty), cal_PN(PN)], axis = 1)
    return final_summary


if __name__ == "__main__":

    file_path = "C:/Users/cs/OneDrive/Desktop/carro/Outgoing/July_24 CARRO USA OUTGOING.xlsx"

    xls = pd.ExcelFile(file_path)

    sheet_names = xls.sheet_names

    print(sheet_names)

    Qty, PN = get_tables(file_path, sheet_names)[0], get_tables(file_path, sheet_names)[1]

    final_summary = merge_tables(Qty, PN)

    final_summary.to_excel('Summary.xlsx')
    

    excel_workbook = load_workbook(file_path)

    # # Create a writer object with the loaded workbook
    # with pd.ExcelWriter(file_path, engine='openpyxl', mode='a') as writer:
    #     writer.book = excel_workbook

    #     # Write the final summary to a new sheet
    #     final_summary.to_excel(writer, sheet_name="Final Summary", index=False)

    #     # Save the writer
    #     writer.save()
