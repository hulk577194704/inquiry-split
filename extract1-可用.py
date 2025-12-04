import os
import pandas as pd
import numpy as np
import json
from typing import List, Dict, Set
import re

class ExcelDataExtractor:
    def __init__(self, folder_path: str, skip_keywords: List[str] = None):
        """
        初始化提取器
        
        Args:
            folder_path: Excel文件所在的文件夹路径
            skip_keywords: 需要跳过的列关键词列表
        """
        self.folder_path = folder_path
        self.skip_keywords = skip_keywords or ["备注", "说明", "附件", "Note", "Description", "Attachment"]
        self.all_data = {}  # 存储所有提取的数据
    
    def is_header_row(self, df: pd.DataFrame, row_index: int) -> bool:
        """
        判断指定行是否为表头行
        
        Args:
            df: DataFrame对象
            row_index: 行索引
            
        Returns:
            bool: 是否为表头行
        """
        if row_index >= len(df) - 1:
            return False
        
        current_row = df.iloc[row_index]
        next_row = df.iloc[row_index + 1]
        
        # 计算当前行中非空单元格数量
        non_empty_cells = current_row.notna()
        if non_empty_cells.sum() == 0:
            return False
        
        # 计算字符串类型单元格的比例
        string_cells = 0
        total_non_empty = 0
        
        for col in range(len(current_row)):
            if pd.notna(current_row.iloc[col]):
                total_non_empty += 1
                cell_value = current_row.iloc[col]
                # 检查是否为字符串类型
                if isinstance(cell_value, str):
                    string_cells += 1
                # 对于其他类型，如果能够转换为字符串且不是纯数字，也认为是字符串
                elif pd.notna(cell_value):
                    try:
                        str_value = str(cell_value).strip()
                        if str_value and not re.match(r'^-?\d+\.?\d*$', str_value):
                            string_cells += 1
                    except:
                        pass
        
        if total_non_empty == 0:
            return False
        
        string_ratio = string_cells / total_non_empty
        
        # 检查下一行是否有数值或非空内容
        next_row_has_content = next_row.notna().sum() > 0
        
        # 如果字符串比例超过50%且下一行有内容，则认为是表头行
        return string_ratio > 0.5 and next_row_has_content
    
    def find_header_row(self, df: pd.DataFrame) -> int:
        """
        查找表头行
        
        Args:
            df: DataFrame对象
            
        Returns:
            int: 表头行索引，如果找不到返回0
        """
        # 在前10行中查找表头行
        for i in range(min(10, len(df) - 1)):
            if self.is_header_row(df, i):
                return i
        return 0  # 如果找不到，默认使用第一行
    
    def process_column_name(self, col_name, next_row_value) -> str:
        """
        处理列名，如果表头列为空，则使用下一行的实际值作为列名
        
        Args:
            col_name: 原始列名
            next_row_value: 下一行对应列的值
            
        Returns:
            str: 处理后的列名
        """
        if pd.isna(col_name) or str(col_name).strip() == '':
            if pd.notna(next_row_value):
                return str(next_row_value).strip()
            else:
                return "Unnamed"
        return str(col_name).strip()
    
    def should_skip_column(self, column_name: str) -> bool:
        """
        判断是否应该跳过该列
        
        Args:
            column_name: 列名
            
        Returns:
            bool: 是否跳过
        """
        if pd.isna(column_name):
            return False
        
        col_name_str = str(column_name).strip()
        for keyword in self.skip_keywords:
            if keyword in col_name_str:
                return True
        return False
    
    def extract_data_from_file(self, file_path: str):
        """
        从单个Excel文件中提取数据
        
        Args:
            file_path: Excel文件路径
        """
        try:
            # 读取Excel文件的第一个工作表
            df = pd.read_excel(file_path, sheet_name=0, header=None)
            print(f"处理文件: {os.path.basename(file_path)}")
            print(f"数据形状: {df.shape}")
            
            if df.empty:
                print(f"警告: 文件 {file_path} 为空")
                return
            
            # 查找表头行
            header_row_idx = self.find_header_row(df)
            print(f"找到表头行: 第{header_row_idx + 1}行")
            
            # 设置表头
            header_row = df.iloc[header_row_idx]
            
            # 处理列名
            processed_headers = []
            for col_idx, header in enumerate(header_row):
                next_row_value = df.iloc[header_row_idx + 1, col_idx] if header_row_idx + 1 < len(df) else None
                processed_header = self.process_column_name(header, next_row_value)
                processed_headers.append(processed_header)
            
            # 重新读取数据，使用处理后的表头
            df_with_header = pd.read_excel(file_path, sheet_name=0, header=header_row_idx)
            df_with_header.columns = processed_headers
            
            # 数据行从表头行的下一行开始
            data_start_row = header_row_idx + 1
            data_df = df_with_header.iloc[data_start_row:].reset_index(drop=True)
            
            print(f"处理后的列名: {list(data_df.columns)}")
            
            # 提取每列的数据
            for column in data_df.columns:
                if self.should_skip_column(column):
                    print(f"跳过列: {column}")
                    continue
                
                # 获取该列的非空数据
                column_data = data_df[column].dropna().tolist()
                
                # 初始化该列的集合（如果尚未存在）
                if column not in self.all_data:
                    self.all_data[column] = set()
                
                # 添加数据到集合中（自动去重）
                for item in column_data:
                    if pd.notna(item):
                        self.all_data[column].add(str(item).strip())
                
                print(f"列 '{column}': 提取了 {len(column_data)} 个值，去重后 {len(self.all_data[column])} 个")
            
            print("-" * 50)
            
        except Exception as e:
            print(f"处理文件 {file_path} 时出错: {str(e)}")
    
    def process_all_files(self):
        """
        处理文件夹中的所有Excel文件
        """
        if not os.path.exists(self.folder_path):
            print(f"错误: 文件夹路径 {self.folder_path} 不存在")
            return
        
        # 获取所有Excel文件
        excel_files = []
        for file in os.listdir(self.folder_path):
            if file.lower().endswith(('.xlsx', '.xls')):
                excel_files.append(os.path.join(self.folder_path, file))
        
        if not excel_files:
            print(f"在文件夹 {self.folder_path} 中未找到Excel文件")
            return
        
        print(f"找到 {len(excel_files)} 个Excel文件")
        print("开始处理文件...")
        print("=" * 50)
        
        # 处理每个文件
        for file_path in excel_files:
            self.extract_data_from_file(file_path)
    
    def save_to_json(self, output_path: str):
        """
        将提取的数据保存为JSON文件
        
        Args:
            output_path: 输出JSON文件路径
        """
        # 将集合转换为列表
        json_data = {}
        for column, values_set in self.all_data.items():
            json_data[column] = sorted(list(values_set))  # 排序以便阅读
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        
        print(f"数据已保存到: {output_path}")
        print(f"总共处理了 {len(json_data)} 列数据")

def main():
    """
    主函数
    """
    # 配置参数
    folder_path = input("请输入Excel文件所在文件夹路径: ").strip()
    
    # 可选的跳过关键词（可以在这里添加或修改）
    skip_keywords = ["备注", "说明", "附件", "Note", "Description", "Attachment"]
    
    # 输出文件路径
    output_file = "extracted_data.json"
    
    # 创建提取器并处理文件
    extractor = ExcelDataExtractor(folder_path, skip_keywords)
    extractor.process_all_files()
    
    if extractor.all_data:
        extractor.save_to_json(output_file)
        
        # 打印统计信息
        print("\n处理完成！统计信息:")
        for column, values in extractor.all_data.items():
            print(f"  {column}: {len(values)} 个唯一值")
    else:
        print("未提取到任何数据")

if __name__ == "__main__":
    main()