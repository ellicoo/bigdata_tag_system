#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL数据源管理类
负责标签规则、现有标签数据的读取和标签结果的写入
"""
import pymysql
import time
from typing import List, Dict, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from ..utils.SparkUdfs import json_to_array


class MysqlMeta:
    """MySQL数据源管理器
    
    职责：
    1. 加载标签规则数据
    2. 加载现有用户标签数据
    3. 写入标签计算结果
    4. 管理MySQL连接和JDBC配置
    """
    
    def __init__(self, spark: SparkSession, mysqlConfig: Dict[str, str]):
        """初始化MySQL数据源管理器
        
        Args:
            spark: Spark会话
            mysqlConfig: MySQL连接配置
        """
        self.spark = spark
        self.mysqlConfig = mysqlConfig
        self.jdbcUrl = self._buildJdbcUrl()

        print(f"🔗 JDBC URL: {self.jdbcUrl}")
        print("🔧 开始MySQL连接测试...")
        
        # 强制执行连接测试并显示结果
        try:
            connection_ok = self.testConnection()
            if connection_ok:
                print("✅ MySQL连接测试通过")
            else:
                print("❌ MySQL连接测试失败，但继续初始化")
        except Exception as e:
            print(f"❌ 连接测试异常: {e}")
            print("⚠️  将继续初始化，但可能影响后续操作")
        
        print("🗄️  MysqlMeta初始化完成")
    
    def _buildJdbcUrl(self) -> str:
        """构建JDBC连接URL"""
        host = self.mysqlConfig['host']
        port = self.mysqlConfig['port']
        database = self.mysqlConfig['database']
        
        # 使用connectionCollation参数支持utf8mb4编码
        # return f"jdbc:mysql://{host}:{port}/{database}?useSSL=false&useUnicode=true&connectionCollation=utf8mb4_unicode_ci&serverTimezone=UTC"
        
        # 添加业务方必要的连接参数：autoReconnect=true & useCursorFetch=true
        return f"jdbc:mysql://{host}:{port}/{database}?useSSL=false&useUnicode=true&connectionCollation=utf8mb4_unicode_ci&autoReconnect=true&useCursorFetch=true&serverTimezone=UTC"
        
        # 完全匹配业务方配置，解决连接问题
        # return f"jdbc:mysql://{host}:{port}/{database}?useUnicode=true&characterEncoding=utf8&useSSL=false&autoReconnect=true&useCursorFetch=true"


    def loadTagRules(self, tagIds: Optional[List[int]] = None) -> DataFrame:
        """加载标签规则DataFrame
        
        Args:
            tagIds: 指定加载的标签ID列表，None表示加载所有活跃标签
            
        Returns:
            DataFrame: 标签规则DataFrame，包含字段：tag_id, rule_conditions, tag_name
        """
        print(f"📋 加载标签规则，指定标签: {tagIds}")
        
        # 构建查询SQL - 更新为新的表结构
        query = """
        (SELECT trc.tag_id, trc.tag_conditions as rule_conditions, td.tag_name, td.description, td.group_attr
         FROM tag_rules_config trc
         INNER JOIN tag_definition td ON trc.tag_id = td.id
         WHERE td.is_active = 1 and td.layer_type = 1 and trc.is_active = 1
        """
        
        if tagIds:
            tagIdsStr = ','.join(map(str, tagIds))
            query += f" AND trc.tag_id IN ({tagIdsStr})"
        
        query += " ORDER BY trc.tag_id) as tag_rules_config"
        
        try:
            rulesDF = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbcUrl) \
                .option("dbtable", query) \
                .option("user", self.mysqlConfig['user']) \
                .option("password", self.mysqlConfig['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
            
            print(f"✅ 标签规则加载完成: {rulesDF.count()} 个标签")
            return rulesDF
            
        except Exception as e:
            print(f"❌ 加载标签规则失败: {e}")
            return self._createEmptyRulesDataFrame()
    
    def loadExistingTags(self) -> DataFrame:
        """加载现有用户标签DataFrame
        
        Returns:
            DataFrame: 现有标签DataFrame，包含字段：user_id, existing_tag_ids(Array)
        """
        print("📖 加载现有用户标签数据...")
        
        query = "(SELECT user_id, tag_id_list FROM user_tag_relation WHERE tag_id_list IS NOT NULL) as existing_tags"
        
        try:
            existingDF = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbcUrl) \
                .option("dbtable", query) \
                .option("user", self.mysqlConfig['user']) \
                .option("password", self.mysqlConfig['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
            
            # 使用SparkUdfs模块转换JSON为Array
            existingDF = existingDF.withColumn(
                "existing_tag_ids",
                json_to_array(col("tag_id_list"))
            ).select("user_id", "existing_tag_ids")
            
            print(f"✅ 现有标签数据加载完成: {existingDF.count()} 个用户")
            return existingDF
            
        except Exception as e:
            print(f"❌ 加载现有标签数据失败: {e}")
            return self._createEmptyExistingTagsDataFrame()
    
    def writeTagResults(self, resultsDF: DataFrame) -> bool:
        """写入标签计算结果到MySQL - Driver收集+分批UPSERT方案
        
        将结果收集到Driver内存，然后使用PyMySQL分批UPSERT到结果表
        完全避免临时表，精确控制IPS，降低数据库压力
        
        Args:
            resultsDF: 结果DataFrame，包含字段：user_id, final_tag_ids_json
            
        Returns:
            bool: 写入是否成功
        """
        print("💾 开始写入标签结果到MySQL（Driver收集+分批UPSERT方案）...")
        
        try:
            # 先检查是否有数据需要写入
            totalCount = resultsDF.count()
            if totalCount == 0:
                print("⚠️  没有结果需要写入")
                return True
            
            print(f"📤 准备写入 {totalCount} 条标签记录...")
            
            # 🚀 步骤1：将结果收集到Driver内存
            print("   📥 收集结果到Driver内存...")
            collect_start_time = time.time()
            
            # 选择需要的字段并收集到Driver
            results_list = resultsDF.select("user_id", col("final_tag_ids_json").alias("tag_id_list")).collect()
            
            collect_time = time.time() - collect_start_time
            print(f"   ✅ 数据收集完成: {len(results_list)} 条记录，耗时 {collect_time:.2f} 秒")
            
            # 🚀 步骤2：分批UPSERT到结果表
            upsert_success = self._executeBatchUpsert(results_list, totalCount)
            
            if upsert_success:
                print(f"✅ 标签结果写入完成: {totalCount} 条记录")
                return True
            else:
                return False
            
        except Exception as e:
            print(f"❌ 写入失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def testConnection(self) -> bool:
        """测试MySQL连接
        
        Returns:
            bool: 连接是否成功
        """
        # 先测试网络连通性
        import socket
        host = self.mysqlConfig['host']
        port = self.mysqlConfig['port']
        
        print(f"🔍 测试网络连通性: {host}:{port}")
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)  # 10秒超时
            result = sock.connect_ex((host, port))
            sock.close()
            
            if result != 0:
                print(f"❌ 网络连接失败: {host}:{port} (错误码: {result})")
                print("   可能原因: 1) 安全组限制 2) 防火墙限制 3) 网络不通")
                return False
            else:
                print(f"✅ 网络连接正常: {host}:{port}")
        except Exception as e:
            print(f"❌ 网络测试异常: {e}")
            return False
        
        # 然后测试MySQL连接
        print("🔍 测试MySQL JDBC连接...")
        try:
            connection = pymysql.connect(**self.mysqlConfig)
            connection.close()
            print("✅ MySQL连接测试成功")
            return True
        except Exception as e:
            print(f"❌ MySQL连接测试失败: {e}")
            return False
    
    def _createEmptyRulesDataFrame(self) -> DataFrame:
        """创建空的标签规则DataFrame"""
        schema = StructType([
            StructField("tag_id", IntegerType(), False),
            StructField("rule_conditions", StringType(), True),
            StructField("tag_name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("group_attr", StringType(), True)
        ])
        
        return self.spark.createDataFrame([], schema)
    
    def _createEmptyExistingTagsDataFrame(self) -> DataFrame:
        """创建空的现有标签DataFrame"""
        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("existing_tag_ids", ArrayType(IntegerType()), True)
        ])
        
        return self.spark.createDataFrame([], schema)
    
    def _executeBatchUpsert(self, results_list: List, total_count: int, batch_size: int = 2000, delay_seconds: float = 0.05) -> bool:
        """分批执行UPSERT操作 - 精确控制IPS，降低数据库压力
        
        Args:
            results_list: Spark collect()返回的Row对象列表
            total_count: 总记录数
            batch_size: 每批处理的记录数，默认2000
            delay_seconds: 批次间延迟时间（秒），默认0.05秒，可控制IPS
            
        Returns:
            bool: 是否成功
        """
        print(f"🔄 开始分批UPSERT，总记录: {total_count}，批大小: {batch_size}，批间延迟: {delay_seconds}s")
        
        # 计算批次数
        import math
        import builtins  # 导入Python内置函数模块
        total_batches = math.ceil(len(results_list) / batch_size)
        successful_batches = 0
        
        try:
            connection = pymysql.connect(**self.mysqlConfig)
            
            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = builtins.min(start_idx + batch_size, len(results_list))  # 使用内置min函数
                batch_records = results_list[start_idx:end_idx]
                
                batch_start_time = time.time()
                
                with connection.cursor() as cursor:
                    # 构建批量UPSERT SQL
                    values_list = []
                    for row in batch_records:
                        user_id = row['user_id']
                        tag_id_list = row['tag_id_list']
                        # 安全处理：转义单引号，防止SQL注入
                        safe_tag_id_list = tag_id_list.replace("'", "''") if tag_id_list else ''
                        values_list.append(f"('{user_id}', '{safe_tag_id_list}')")
                    
                    batch_sql = f"""
                    INSERT INTO user_tag_relation (user_id, tag_id_list) VALUES 
                    {', '.join(values_list)}
                    ON DUPLICATE KEY UPDATE
                        updated_time = CASE 
                            WHEN JSON_EXTRACT(user_tag_relation.tag_id_list, '$') <> JSON_EXTRACT(VALUES(tag_id_list), '$')
                            THEN CURRENT_TIMESTAMP 
                            ELSE user_tag_relation.updated_time 
                        END,
                        tag_id_list = VALUES(tag_id_list)
                    """
                    
                    cursor.execute(batch_sql)
                    affected_rows = cursor.rowcount
                    connection.commit()
                    
                    batch_time = time.time() - batch_start_time
                    successful_batches += 1
                    
                    # 进度显示
                    progress_percent = (batch_idx + 1) / total_batches * 100
                    print(f"   ✅ 批次 {batch_idx + 1}/{total_batches} ({progress_percent:.1f}%): "
                          f"{len(batch_records)} 条记录，影响 {affected_rows} 行，耗时 {batch_time:.2f}s")
                
                # 批次间延迟，控制IPS
                if batch_idx < total_batches - 1 and delay_seconds > 0:
                    time.sleep(delay_seconds)
            
            connection.close()
            print(f"   🎉 分批UPSERT完成: {successful_batches}/{total_batches} 批次成功")
            return successful_batches == total_batches
            
        except Exception as e:
            print(f"   ❌ 分批UPSERT失败: {e}")
            import traceback
            traceback.print_exc()
            return False
