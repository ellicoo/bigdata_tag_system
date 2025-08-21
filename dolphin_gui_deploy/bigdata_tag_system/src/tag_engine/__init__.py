#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
标签计算引擎包
简化的PySpark实现，基于Spark DSL + UDF架构
"""

from .engine.TagEngine import TagEngine
from .engine.TagGroup import TagGroup
from .meta.HiveMeta import HiveMeta
from .meta.MysqlMeta import MysqlMeta
from .parser.TagRuleParser import TagRuleParser
from .utils import SparkUdfs

__all__ = [
    "TagEngine",
    "TagGroup",
    "HiveMeta", 
    "MysqlMeta",
    "TagRuleParser",
    "SparkUdfs"
]

__version__ = "1.0.0"
__author__ = "Tag System Team"
__description__ = "简化PySpark标签计算系统 - 纯Spark DSL + 模块函数实现"