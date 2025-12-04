import os
import pandas as pd
import json
import re
from typing import Dict, List, Set, Any
import warnings
warnings.filterwarnings('ignore')

class ExcelColumnExtractor:
    def __init__(self, keywords: List[str] = None, white_list_keywords: List[str] = None):
        """
        初始化提取器
        
        Args:
            keywords: 需要跳过的列关键词列表
        """
        self.keywords = keywords
        self.white_list_keywords = white_list_keywords
        self.all_data: Dict[str, Set[Any]] = {}
    def contains_chinese(self, text):
        """检查字符串是否包含中文"""
        return bool(re.search('[\u4e00-\u9fff]', text))
    def is_in_white_list(self, column_name: str) -> bool:
        """
        判断该列是否在白名单中 
        
        Args:
            column_name: 列名
            
        Returns:
            bool: 否在白名单中 
        """
        if pd.isna(column_name) or column_name == '':
            return False;
            
        column_name_str = str(column_name)
        for keyword in self.white_list_keywords:
            if keyword.lower() in column_name_str.lower():
                return True
        return False
    def read_json_file(self):
        """读取JSON文件并返回数据"""
        current_working_dir = os.path.join(os.getcwd(), "extracted_columns.json")
        try:
            with open(current_working_dir, 'r', encoding='utf-8') as file:
                data = json.load(file)
            return data
        except Exception as e:
            print(f"读取文件时出错: {e}")
            return None
    
    def is_header_row(self, df_slice: pd.DataFrame, row_index: int) -> bool:
        """
        判断某行是否为表头行
        
        Args:
            df_slice: DataFrame切片（前10行）
            row_index: 当前行索引
            
        Returns:
            bool: 是否为表头行
        """
        if row_index >= len(df_slice) - 1:
            return False
            
        current_row = df_slice.iloc[row_index]
        next_row = df_slice.iloc[row_index + 1]
        
        # 计算当前行非空单元格数量
        non_empty_cells = current_row.dropna()
        if len(non_empty_cells) == 0:
            return False
            
        # 计算字符串类型单元格比例
        string_cells = 0
        for cell in non_empty_cells:
            if isinstance(cell, str):
                string_cells += 1
        
        string_ratio = string_cells / len(non_empty_cells)
        
        # 检查下一行是否有数值或非空内容
        next_row_non_empty = next_row.dropna()
        has_next_row_data = len(next_row_non_empty) > 0
        
        # 判断条件：字符串比例超过50%且下一行有数据
        return string_ratio > 0.5 and has_next_row_data
    
    def find_header_row(self, df: pd.DataFrame) -> int:
        """
        查找表头行
        
        Args:
            df: DataFrame
            
        Returns:
            int: 表头行索引
        """
        # 只检查前10行
        check_rows = min(10, len(df))
        
        for i in range(check_rows - 1):
            if self.is_header_row(df.iloc[:check_rows], i):
                return i
        
        # 如果没有找到符合条件表头，默认使用第一行
        return 0
    
    def process_header(self, header_row: pd.Series, first_data_row: pd.Series) -> List[str]:
        """
        处理表头，填充空值
        
        Args:
            header_row: 表头行
            first_data_row: 第一行数据
            
        Returns:
            List[str]: 处理后的列名列表
        """
        column_names = []
        
        for idx, header_cell in enumerate(header_row):
            if pd.isna(header_cell) or header_cell == '':
                # 如果表头为空，使用下一行对应列的值作为列名
                if idx < len(first_data_row) and not pd.isna(first_data_row.iloc[idx]):
                    column_name = str(first_data_row.iloc[idx])
                else:
                    column_name = f"Column_{idx}"
            else:
                column_name = str(header_cell)
            
            column_names.append(column_name)
        
        return column_names
    
    def should_skip_column(self, column_name: str) -> bool:
        """
        判断是否应该跳过该列
        
        Args:
            column_name: 列名
            
        Returns:
            bool: 是否跳过
        """
        if pd.isna(column_name) or column_name == '':
            return False
            
        column_name_str = str(column_name)
        for keyword in self.keywords:
            if keyword.lower() in column_name_str.lower():
                return True
        if not self.contains_chinese(column_name_str): 
            return True
        return False
    
    def process_excel_file(self, file_path: str):
        """
        处理单个Excel文件
        
        Args:
            file_path: Excel文件路径
        """
        try:
            # 获取工作表数量和信息
            excel_file = pd.ExcelFile(file_path)
            num_sheets = len(excel_file.sheet_names)

            for i in range(num_sheets):
                # 读取Excel文件的第一个工作表
                df = pd.read_excel(file_path, sheet_name=i, header=None)
                
                if df.empty:
                    print(f"警告: 文件 {file_path} 为空")
                    return
                
                # 查找表头行
                header_row_index = self.find_header_row(df)
                print(f"文件 {os.path.basename(file_path)}: 表头行索引为 {header_row_index}")
                
                # 获取表头行和第一行数据
                header_row = df.iloc[header_row_index]
                data_start_row = header_row_index + 1
                
                if data_start_row >= len(df):
                    print(f"警告: 文件 {file_path} 没有数据行")
                    return
                
                first_data_row = df.iloc[data_start_row]
                
                # 处理表头
                column_names = self.process_header(header_row, first_data_row)
                
                # 读取数据（跳过表头行）
                data_df = pd.read_excel(file_path, sheet_name=0, header=None, 
                                    skiprows=data_start_row)
                
                # 处理每一列
                for col_idx, col_name in enumerate(column_names):
                    if col_idx >= data_df.shape[1]:
                        continue
                        
                    if self.should_skip_column(col_name):
                        continue
                    if not self.is_in_white_list(col_name):
                        continue
                    # 获取该列数据并去重
                    column_data = data_df.iloc[:, col_idx].dropna()
                    
                    # 转换为合适的类型并去重
                    processed_data = set()
                    for item in column_data:
                        if pd.isna(item):
                            continue
                        
                        # 尝试转换为数值类型
                        try:
                            if isinstance(item, (int, float)):
                                continue
                            else:
                                processed_item = float(item) if '.' in str(item) else int(item)
                                continue
                        except (ValueError, TypeError):
                            item = str(item).strip()
                            if self.contains_chinese(item):
                                continue
                            if item == '-':
                                continue
                            # 如果转换失败，保持原样
                            processed_data.add(item)
                    
                    # 合并到总数据中
                    if col_name not in self.all_data:
                        self.all_data[col_name] = set()

                    if col_name == '标准' and self.all_data['标准'] is not None:
                        for item in processed_data.copy():
                            for item2 in self.all_data['标准']:
                                if item2 in item:
                                    processed_data.remove(item)
                                    break

                    self.all_data[col_name].update(processed_data)
                
        except Exception as e:
            print(f"处理文件 {file_path} 时出错: {str(e)}")
    
    def process_folder(self, folder_path: str):
        """
        处理文件夹中的所有Excel文件
        
        Args:
            folder_path: 文件夹路径
        """
        if not os.path.exists(folder_path):
            print(f"错误: 文件夹路径 {folder_path} 不存在")
            return
        
        # 查找所有Excel文件
        excel_files = []
        for file in os.listdir(folder_path):
            if file.endswith(('.xlsx', '.xls')):
                excel_files.append(os.path.join(folder_path, file))
        
        if not excel_files:
            print(f"在文件夹 {folder_path} 中未找到Excel文件")
            return
        
        print(f"找到 {len(excel_files)} 个Excel文件")

        orign_map_data = self.read_json_file()
        if orign_map_data is not None:
            self.all_data = orign_map_data
            for col_name, values in orign_map_data.items():
                self.all_data[col_name] = set(values)
        # 处理每个文件
        for file_path in excel_files:
            print(f"正在处理: {os.path.basename(file_path)}")
            self.process_excel_file(file_path)

        for col_name, values in self.all_data.items():
            self.all_data[col_name] = sorted(values)
        self.all_data = {key: value for key, value in self.all_data.items() if value}
        
    
    def save_to_json(self, output_path: str):
        """
        将结果保存为JSON文件
        
        Args:
            output_path: 输出JSON文件路径
        """

        
        # 转换为可序列化的格式
        # json_data = {}
        # for col_name, values in self.all_data.items():
        #     # 将set转换为list，并对数值进行排序
        #     sorted_values = sorted(list(values), key=lambda x: str(x))
        #     json_data[col_name] = sorted_values
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(self.all_data, f, ensure_ascii=False, indent=2)
        
        print(f"结果已保存到: {output_path}")
        
        # # 打印统计信息
        # print("\n统计信息:")
        # for col_name, values in json_data.items():
        #     print(f"  {col_name}: {len(values)} 个唯一值")

def main():
    """
    主函数
    """

    # 用户输入文件夹路径
        # folder_path = input("请输入包含Excel文件的文件夹路径: ").strip()
    folder_path = r"C:\Users\57719\Desktop\询盘汇总"
    
    # 可选的过滤关键词（可以修改）
    skip_keywords = ["Description", "描述", "NO", "S/N", "UNIT", "QTY", "UoM","ITEM"]

    white_list_keywords = ["工艺","外径","壁厚","材质","标准","压力","尺寸","名称","连接面"]
    
    # 创建提取器
    extractor = ExcelColumnExtractor(keywords=skip_keywords, white_list_keywords=white_list_keywords)
    
    # 处理文件夹
    extractor.process_folder(folder_path)
    
    if not extractor.all_data:
        print("没有提取到任何数据")
        return
    
    # 生成输出文件路径
    output_file = os.path.join(os.getcwd(), "extracted_columns.json")
    
    # 保存结果
    extractor.save_to_json(output_file)
    
    # 显示提取的列信息
    print("\n提取的列:")
    for col_name in extractor.all_data.keys():
        print(f"  - {col_name}")

if __name__ == "__main__":
    main()