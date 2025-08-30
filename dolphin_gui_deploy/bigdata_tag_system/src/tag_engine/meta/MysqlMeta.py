#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL数据源管理类
负责标签规则、现有标签数据的读取和标签结果的写入
"""
import pymysql
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

        # 创建临时库配置
        self.mysqlConfigTemp = mysqlConfig.copy()
        self.mysqlConfigTemp['database'] = 'bigdata'  # 修改数据库名为bigdata
        self.jdbcUrlTemp = self._buildJdbcUrlTemp()  # 构建指向bigdata的JDBC URL

        print(f"🔗 主库JDBC URL: {self.jdbcUrl}")
        print(f"🔗 临时库JDBC URL: {self.jdbcUrlTemp}")

        # print(f"🔗 JDBC URL: {self.jdbcUrl}")
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

    def _buildJdbcUrlTemp(self) -> str:
        """构建指向临时库的JDBC连接URL"""
        host = self.mysqlConfig['host']
        port = self.mysqlConfig['port']
        database = 'bigdata'  # 固定使用bigdata库

        return f"jdbc:mysql://{host}:{port}/{database}?useSSL=false&useUnicode=true&connectionCollation=utf8mb4_unicode_ci&autoReconnect=true&useCursorFetch=true&serverTimezone=UTC"

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
        (SELECT trc.tag_id, trc.tag_conditions as rule_conditions, td.tag_name, td.description
         FROM tag_rules_config trc
         LEFT JOIN tag_definition td ON trc.tag_id = td.id
         WHERE td.is_active = 1
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
        """写入标签计算结果到MySQL - 临时表+原生SQL方案
        
        使用Spark原生JDBC写入临时表，然后用纯SQL执行UPSERT
        完全避免Python版本冲突问题
        
        Args:
            resultsDF: 结果DataFrame，包含字段：user_id, final_tag_ids_json
            
        Returns:
            bool: 写入是否成功
        """
        print("💾 开始写入标签结果到MySQL（临时表+原生SQL方案）...")
        
        try:
            # 先检查是否有数据需要写入
            totalCount = resultsDF.count()
            if totalCount == 0:
                print("⚠️  没有结果需要写入")
                return True
            
            print(f"📤 准备写入 {totalCount} 条标签记录...")
            
            # 🚀 步骤1：写入临时表（只要user_id, final_tag_ids_json）
            # import time
            # temp_table = f"user_tags_temp_{int(time.time())}"
            # print(f"📋 创建临时表: {temp_table}")

            # 🚀 步骤1：在bigdata库中创建临时表
            import time
            temp_table = f"user_tags_temp_{int(time.time())}"
            full_temp_table_name = f"bigdata.{temp_table}"  # 使用完整表名
            print(f"📋 创建临时表: {full_temp_table_name}")
            
            # 使用Spark原生JDBC写入，完全避免Python代码分发
            # resultsDF.select("user_id", col("final_tag_ids_json").alias("tag_id_list")) \
            #     .write \
            #     .format("jdbc") \
            #     .option("url", self.jdbcUrl) \
            #     .option("dbtable", temp_table) \
            #     .option("user", self.mysqlConfig['user']) \
            #     .option("password", self.mysqlConfig['password']) \
            #     .option("driver", "com.mysql.cj.jdbc.Driver") \
            #     .option("createTableOptions", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4") \
            #     .mode("overwrite") \
            #     .save()
            #
            # print(f"✅ 临时表 {temp_table} 写入完成")

            # 使用指向bigdata库的JDBC URL创建临时表
            resultsDF.select("user_id", col("final_tag_ids_json").alias("tag_id_list")) \
                .write \
                .format("jdbc") \
                .option("url", self.jdbcUrlTemp)  \
                .option("dbtable", temp_table)  \
                .option("user", self.mysqlConfig['user']) \
                .option("password", self.mysqlConfig['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("createTableOptions", "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4") \
                .mode("overwrite") \
                .save()

            print(f"✅ 临时表 {full_temp_table_name} 写入完成")
            
            # # 🚀 步骤2：执行UPSERT
            # upsert_success = self._executeSimpleUpsert(temp_table, totalCount)
            #
            # # 🚀 步骤3：清理临时表
            # self._dropTempTable(temp_table)

            # 🚀 步骤2：执行UPSERT（使用完整表名bigdata.temp_table）
            upsert_success = self._executeCrossDbUpsert(full_temp_table_name, totalCount)

            # 🚀 步骤3：清理临时表（使用临时库配置）
            self._dropCrossDbTempTable(temp_table)
            
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
            StructField("description", StringType(), True)
        ])
        
        return self.spark.createDataFrame([], schema)
    
    def _createEmptyExistingTagsDataFrame(self) -> DataFrame:
        """创建空的现有标签DataFrame"""
        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("existing_tag_ids", ArrayType(IntegerType()), True)
        ])
        
        return self.spark.createDataFrame([], schema)

    def _executeCrossDbUpsert(self, full_temp_table_name: str, record_count: int) -> bool:
        """执行跨库UPSERT操作"""
        print(f"🔄 执行跨库UPSERT操作，从 {full_temp_table_name} 到 user_tag_relation...")

        try:
            # 使用主库连接（biz_user）
            connection = pymysql.connect(**self.mysqlConfig)

            with connection.cursor() as cursor:
                # 使用完整表名引用临时表
                upsert_sql = f"""
                INSERT INTO user_tag_relation (user_id, tag_id_list)
                SELECT user_id, tag_id_list
                FROM {full_temp_table_name}
                ON DUPLICATE KEY UPDATE
                    updated_time = CASE 
                        WHEN JSON_EXTRACT(user_tag_relation.tag_id_list, '$') <> JSON_EXTRACT(VALUES(tag_id_list), '$')
                        THEN CURRENT_TIMESTAMP 
                        ELSE user_tag_relation.updated_time 
                    END,
                    tag_id_list = VALUES(tag_id_list)
                """

                print(f"   📝 执行SQL: INSERT INTO user_tag_relation ... FROM {full_temp_table_name}")
                cursor.execute(upsert_sql)
                affected_rows = cursor.rowcount
                connection.commit()

                print(f"   ✅ UPSERT完成，影响行数: {affected_rows}")
                return True

        except Exception as e:
            print(f"   ❌ UPSERT失败: {e}")
            import traceback
            traceback.print_exc()
            return False
        finally:
            connection.close()
    
    # def _executeSimpleUpsert(self, temp_table: str, record_count: int) -> bool:
    #     """执行简单的UPSERT，利用现有的user_tag_relation表结构"""
    #     print(f"🔄 执行UPSERT操作，从 {temp_table} 到 user_tag_relation...")
    #
    #     try:
    #         connection = pymysql.connect(**self.mysqlConfig)
    #
    #         with connection.cursor() as cursor:
    #             # 简单UPSERT，保持原有表结构和业务逻辑
    #             upsert_sql = f"""
    #             INSERT INTO user_tag_relation (user_id, tag_id_list)
    #             SELECT user_id, tag_id_list
    #             FROM {temp_table}
    #             ON DUPLICATE KEY UPDATE
    #                 updated_time = CASE
    #                     WHEN JSON_EXTRACT(user_tag_relation.tag_id_list, '$') <> JSON_EXTRACT(VALUES(tag_id_list), '$')
    #                     THEN CURRENT_TIMESTAMP
    #                     ELSE user_tag_relation.updated_time
    #                 END,
    #                 tag_id_list = VALUES(tag_id_list)
    #             """
    #
    #             print(f"   📝 执行SQL: INSERT INTO user_tag_relation ... FROM {temp_table}")
    #             cursor.execute(upsert_sql)
    #             affected_rows = cursor.rowcount
    #             connection.commit()
    #
    #             print(f"   ✅ UPSERT完成，影响行数: {affected_rows}")
    #             return True
    #
    #     except Exception as e:
    #         print(f"   ❌ UPSERT失败: {e}")
    #         import traceback
    #         traceback.print_exc()
    #         return False
    #     finally:
    #         connection.close()
    
    # def _dropTempTable(self, temp_table: str):
    #     """清理临时表"""
    #     print(f"🧹 清理临时表: {temp_table}")
    #
    #     try:
    #         connection = pymysql.connect(**self.mysqlConfig)
    #
    #         with connection.cursor() as cursor:
    #             cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
    #             connection.commit()
    #             print(f"   ✅ 临时表 {temp_table} 已清理")
    #
    #     except Exception as e:
    #         print(f"   ⚠️  清理临时表失败: {e}")
    #     finally:
    #         connection.close()

    def _dropCrossDbTempTable(self, temp_table: str):
        """清理跨库临时表"""
        print(f"🧹 清理临时表: bigdata.{temp_table}")

        try:
            # 使用临时库配置（bigdata）连接
            connection = pymysql.connect(**self.mysqlConfigTemp)

            with connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
                connection.commit()
                print(f"   ✅ 临时表 bigdata.{temp_table} 已清理")

        except Exception as e:
            print(f"   ⚠️  清理临时表失败: {e}")
        finally:
            connection.close()