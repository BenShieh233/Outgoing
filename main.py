import os
import re
import pandas as pd
import glob
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed


def read_excel_files():

    try:
        month_pattern = r"^(January|February|March|April|May|June|July|August|September|October|November|December)_.*\.xlsx$"
        
        pattern = re.compile(month_pattern)
        
        current_path = os.getcwd()
        
        all_files = glob.glob(os.path.join(current_path, "*.xlsx"))
        
        excel_files = [file for file in all_files if pattern.match(os.path.basename(file))]

        print(f"已获取当前路径下的月度表格：{excel_files}")
        
        return excel_files
    
    except:
        print("未发现当前路径下的月度表格，请检查文件路径")
        return None

def data_preprocessing(file_path):

    xls = pd.ExcelFile(file_path)

    sheet_names = xls.sheet_names

    if len(sheet_names) < 3:
        print("请检查表格数量是否至少为3")
        return None

    df = pd.concat([pd.read_excel(file_path, sheet_name=sheet_name) for sheet_name in sheet_names[2:]], axis=0)
    print("已读取所有表格")
    df = df[['SKU', 'Wayfair SKU', 'Qty', 'PROBLEM NUMBER']]
    df = df.dropna(subset=['PROBLEM NUMBER'])

    # 清楚所有输入的前后空格
    df['SKU'] = df['SKU'].str.strip()
    df['Wayfair SKU'] = df['Wayfair SKU'].str.strip()
    df = df.dropna(subset=['PROBLEM NUMBER'])
    df['PROBLEM NUMBER'] = df['PROBLEM NUMBER'].apply(lambda x: x.strip() if isinstance(x, str) else x)
    df['Problem Number'] = df['PROBLEM NUMBER'].apply(lambda x: str(int(x)) if isinstance(x, (float, int)) and not np.isnan(x) else x)

    try:

        # 正则表达式
        valid_pattern = re.compile(r'^\'*\d+\'*([-,]\s*\d+)*\'*$|^\d+([-,]\d+)*\s*[,\']?$')

        df = df[df['Problem Number'].apply(lambda x: bool(valid_pattern.match(x)))]

        print("已筛选所有符合格式的字符串")
        # Strip all whitespace and remove single quotes from 'Problem Number'
        df['Problem Number'] = df['Problem Number'].str.replace("'", "").str.replace(" ", "")
        
        # Replace commas with hyphens in 'Problem Number'
        df['Problem Number'] = df['Problem Number'].str.replace(",", "-")
        print("已转换字符串格式并去除空格")


    except Exception as e:
        print(f"字符串检索失败，请检查错误：{e}")

    df = df[['SKU', 'Wayfair SKU', 'Qty', 'Problem Number']]

    return df
    
        
def get_tables(df):

    Qty = df[['SKU', 'Wayfair SKU', 'Qty']] 

    PN = df[['SKU', 'Wayfair SKU', 'Qty', 'Problem Number']]

    return Qty, PN


def cal_qty(df):
    summary = df.groupby(['SKU', 'Wayfair SKU']).agg(total_qty=('Qty', 'sum')).reset_index()
    return summary

def cal_PN(df):
    try:
        df = df.assign(Problem_Number=df['Problem Number'].str.split('-')).explode('Problem_Number')
        df_PN = df[['SKU', 'Wayfair SKU', 'Qty', 'Problem_Number']]
        print("已分隔多种Problem Number至不同列")

    except Exception as e:
        print(f"请检查Problem Number的格式，报错：{e}")
    
    try:
        grouped = df_PN.groupby(['SKU', 'Wayfair SKU', 'Problem_Number']).agg(total_qty=('Qty', 'sum')).reset_index()
        pivot_table = grouped.pivot_table(index=['SKU', 'Wayfair SKU'], columns='Problem_Number', values='total_qty', fill_value=0).reset_index()
        pivot_table.columns = ['SKU', 'Wayfair SKU'] + [f'PN-{col}' for col in pivot_table.columns[2:]]
        pivot_table = pivot_table.reindex(sorted(pivot_table.columns[2:], key=lambda x: int(x.split('-')[1])), axis=1)

    except:
        print("无法处理非integer类型的值，请检查Qty列的格式")
    return pivot_table

def merge_tables(Qty, PN):
    try:
        final_summary = pd.concat([cal_qty(Qty), cal_PN(PN)], axis = 1)
        final_summary = final_summary.replace(0, np.nan)
        print("已合并Qty与Problem Number表格")
        return final_summary

    except Exception as e:
        print("合并出错，请检查表格格式")


def process_file(file_path):
    print(f"当前正在处理表格 {os.path.basename(file_path)}")
    df = data_preprocessing(file_path)
    if df is not None:
        df_qty, df_pn = get_tables(df)
        summary = merge_tables(df_qty, df_pn)

        if summary is not None:

            summary.to_excel(f"Summary_{os.path.basename(file_path)}", index=False)
            print(f"已完成对表格 {os.path.basename(file_path)} 的处理")

        else:
            print(f"处理表格 {os.path.basename(file_path)} 时出错")
    else:
        print(f"处理表格 {os.path.basename(file_path)} 时出错")


def main():
    excel_files = read_excel_files()
    if excel_files:
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(process_file, file_path) for file_path in excel_files]
            for future in as_completed(futures):
                future.result()
        input("输入任何按键以退出程序...")

if __name__ == '__main__':
    main()





