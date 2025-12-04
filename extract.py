import os
import pandas as pd
import json
from collections import defaultdict
import glob

def extract_unique_values_from_excels(folder_path):
    """
    提取文件夹下所有Excel文件中除DESCRIPTION列外的所有列的去重值
    
    Args:
        folder_path (str): Excel文件所在文件夹路径
    
    Returns:
        dict: 包含所有列去重值的字典
    """
    # 支持的文件扩展名
    excel_extensions = ['*.xlsx', '*.xls', '*.xlsm']
    
    # 获取所有Excel文件路径
    excel_files = []
    for extension in excel_extensions:
        excel_files.extend(glob.glob(os.path.join(folder_path, extension)))
    
    if not excel_files:
        print(f"在文件夹 {folder_path} 中未找到Excel文件")
        return {}
    
    # 用于存储所有列的去重值
    all_unique_values = defaultdict(set)
    
    for file_path in excel_files:
        print(f"正在处理文件: {os.path.basename(file_path)}")
        
        try:
            # 读取Excel文件的所有sheet
            excel_file = pd.ExcelFile(file_path)
            
            for sheet_name in excel_file.sheet_names:
                try:
                    # 读取sheet数据
                    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
                    
                    if df.empty:
                        continue
                    
                    # 尝试自动检测表头行
                    header_row = find_header_row(df)
                    
                    if header_row is not None:
                        # 重新读取数据，使用检测到的表头
                        df = pd.read_excel(file_path, sheet_name=sheet_name, header=header_row)
                        
                        # 处理列名，确保都是字符串且去除前后空格
                        df.columns = [str(col).strip() if pd.notna(col) else f"Column_{i}" 
                                    for i, col in enumerate(df.columns)]
                        
                        # 排除DESCRIPTION列（不区分大小写）
                        columns_to_process = [col for col in df.columns 
                                            if 'DESCRIPTION'.lower() in str(col).lower ||  ]
                        
                        # 提取每列的非空唯一值
                        for column in columns_to_process:
                            # 获取非空值并转换为字符串
                            non_null_values = df[column].dropna().astype(str)
                            # 去除前后空格并添加到集合中
                            unique_vals = set(val.strip() for val in non_null_values if val.strip())
                            all_unique_values[column].update(unique_vals)
                            
                    else:
                        print(f"  在Sheet '{sheet_name}' 中未找到合适的表头，跳过处理")
                        
                except Exception as e:
                    print(f"  处理Sheet '{sheet_name}' 时出错: {str(e)}")
                    continue
                    
        except Exception as e:
            print(f"处理文件 {os.path.basename(file_path)} 时出错: {str(e)}")
            continue
    
    # 将集合转换为排序后的列表
    result = {}
    for column, values_set in all_unique_values.items():
        # 对值进行排序
        sorted_values = sorted(list(values_set))
        result[column] = sorted_values
    
    return result

def find_header_row(df, sample_size=5):
    """
    自动检测表头行
    
    Args:
        df (pd.DataFrame): 数据框
        sample_size (int): 检查的行数
    
    Returns:
        int or None: 表头行索引，如果未找到返回None
    """
    # 检查前几行，找到包含最多非空文本值的行
    best_row = None
    max_text_count = 0
    
    for i in range(min(sample_size, len(df))):
        row = df.iloc[i]
        # 计算该行中非空文本值的数量
        text_count = sum(1 for val in row if pd.notna(val) and str(val).strip())
        
        if text_count > max_text_count:
            max_text_count = text_count
            best_row = i
    
    # 如果找到包含足够多非空值的行，认为是表头
    if max_text_count >= len(df.columns) * 0.5:  # 至少50%的列有数据
        return best_row
    
    return None

def save_to_json(data, output_file):
    """保存数据到JSON文件"""
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def main():
    # 配置参数
    folder_path = input("请输入Excel文件所在文件夹路径: ").strip()
    
    if not os.path.exists(folder_path):
        print("文件夹路径不存在！")
        return
    
    # 提取数据
    print("开始提取数据...")
    unique_values_dict = extract_unique_values_from_excels(folder_path)
    
    if not unique_values_dict:
        print("未提取到任何数据！")
        return
    
    # 显示提取结果
    print("\n提取到的列及其唯一值数量:")
    for column, values in unique_values_dict.items():
        print(f"  {column}: {len(values)} 个唯一值")
    
    # 保存为JSON文件
    output_file = os.path.join(folder_path, "extracted_unique_values.json")
    save_to_json(unique_values_dict, output_file)
    
    print(f"\n数据已保存到: {output_file}")
    
    # 显示部分结果预览
    print("\n预览部分结果:")
    for i, (column, values) in enumerate(unique_values_dict.items()):
        if i < 3:  # 只显示前3列
            preview_values = values[:5]  # 每列显示前5个值
            print(f"  {column}: {preview_values}")

if __name__ == "__main__":
    main()