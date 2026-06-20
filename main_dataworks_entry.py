#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DataWorks 主程序入口 - 用于单独上传到资源中心
此文件应该上传到：bigdata_tag_system/main_dataworks.py

工作原理：
1. 此文件在资源中心独立存在（不在 ZIP 包内）
2. bigdata_tag_system.zip 通过 cupid.resources 下载到工作目录
3. PYTHONPATH 设置为 ./bigdata_tag_system/src
4. 此脚本直接使用 PYTHONPATH 中的路径导入模块
"""
import sys
import os

# DataWorks 环境下，Archive 类型的 ZIP 会被自动解压成目录
# 实际路径：./bigdata_tag_system/src（不是 ./bigdata_tag_system.zip/src）
paths_to_add = [
    './bigdata_tag_system/src',       # Archive 解压后的路径（正确）
    'bigdata_tag_system/src',         # 备用路径
    './bigdata_tag_system.zip/src',   # 如果 ZIP 未被解压（备用）
    './src',                          # 备用
    'src'                             # 备用
]

for path in paths_to_add:
    if path not in sys.path:
        sys.path.insert(0, path)
        print(f"✅ 添加路径到 sys.path: {path}")

# 导入并运行实际的主程序
if __name__ == "__main__":
    try:
        from tag_engine.main_dataworks import main
        main()
    except ImportError as e:
        print(f"❌ 导入错误: {e}")
        print(f"📁 当前工作目录: {os.getcwd()}")
        print(f"📋 sys.path: {sys.path}")
        print(f"📂 当前目录内容:")
        try:
            for item in os.listdir('.'):
                print(f"  - {item}")
        except Exception as list_err:
            print(f"  无法列出目录: {list_err}")
        raise