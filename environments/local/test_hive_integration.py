#!/usr/bin/env python3
"""
Hive表集成测试脚本
验证真实的S3/Hive数据读取和标签计算功能
"""

import sys
import os
import logging
from datetime import datetime

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class HiveIntegrationTester:
    """Hive集成测试器"""
    
    def __init__(self):
        """初始化测试器"""
        logger.info("🧪 初始化Hive集成测试器")
        
        from src.common.config.manager import ConfigManager
        self.config = ConfigManager.load_config('local')
        self.spark = None
        
    def create_spark_session(self):
        """创建Spark会话"""
        from pyspark.sql import SparkSession
        
        # 获取JAR文件路径
        jars_dir = os.path.join(os.path.dirname(__file__), "jars")
        jar_files = [
            os.path.join(jars_dir, "mysql-connector-j-8.0.33.jar"),
            os.path.join(jars_dir, "hadoop-aws-3.3.4.jar"),
            os.path.join(jars_dir, "aws-java-sdk-bundle-1.12.262.jar")
        ]
        existing_jars = [jar for jar in jar_files if os.path.exists(jar)]
        
        self.spark = SparkSession.builder \
            .appName("HiveIntegrationTest") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.jars", ",".join(existing_jars) if existing_jars else "") \
            .getOrCreate()
        
        logger.info("✅ Spark会话创建成功")
        return self.spark
    
    def test_s3_connectivity(self):
        """测试S3连接"""
        logger.info("🔍 测试S3连接...")
        
        try:
            import boto3
            from botocore.exceptions import ClientError
            
            s3_client = boto3.client(
                's3',
                endpoint_url='http://localhost:9000',
                aws_access_key_id='minioadmin',
                aws_secret_access_key='minioadmin',
                region_name='us-east-1'
            )
            
            # 列出存储桶
            buckets = s3_client.list_buckets()
            logger.info(f"✅ S3连接成功，存储桶: {[b['Name'] for b in buckets.get('Buckets', [])]}")
            
            # 列出test-data-lake中的对象
            try:
                objects = s3_client.list_objects_v2(Bucket='test-data-lake', Prefix='warehouse/')
                if 'Contents' in objects:
                    logger.info(f"📊 warehouse目录包含 {len(objects['Contents'])} 个对象")
                    for obj in objects['Contents'][:5]:  # 显示前5个对象
                        logger.info(f"   - {obj['Key']} ({obj['Size']} bytes)")
                else:
                    logger.warning("⚠️ warehouse目录为空")
                    return False
                    
            except ClientError as e:
                logger.error(f"❌ 存储桶test-data-lake访问失败: {e}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"❌ S3连接测试失败: {e}")
            return False
    
    def test_hive_table_reading(self):
        """测试Hive表读取"""
        logger.info("📖 测试Hive表读取...")
        
        if not self.spark:
            self.create_spark_session()
        
        tables_to_test = [
            ('user_basic_info', ['user_id', 'age', 'user_level', 'kyc_status']),
            ('user_asset_summary', ['user_id', 'total_asset_value', 'cash_balance']),
            ('user_activity_summary', ['user_id', 'trade_count_30d', 'risk_score'])
        ]
        
        test_results = {}
        
        for table_name, columns in tables_to_test:
            try:
                logger.info(f"📋 测试表: {table_name}")
                
                # 读取完整表
                full_path = f"s3a://test-data-lake/warehouse/{table_name}"
                df = self.spark.read.parquet(full_path)
                
                total_count = df.count()
                logger.info(f"   总记录数: {total_count:,}")
                
                # 读取指定列
                selected_df = df.select(*columns)
                selected_count = selected_df.count()
                logger.info(f"   选择列后记录数: {selected_count:,}")
                
                # 显示schema
                logger.info(f"   表结构: {[f.name for f in df.schema.fields]}")
                
                # 显示前几条记录
                sample_data = selected_df.limit(3).collect()
                logger.info(f"   样本数据: {len(sample_data)} 条")
                for i, row in enumerate(sample_data):
                    logger.info(f"     [{i+1}] {row.asDict()}")
                
                test_results[table_name] = {
                    'success': True,
                    'count': total_count,
                    'columns': len(df.columns)
                }
                
                logger.info(f"✅ 表 {table_name} 读取测试通过")
                
            except Exception as e:
                logger.error(f"❌ 表 {table_name} 读取测试失败: {e}")
                test_results[table_name] = {
                    'success': False,
                    'error': str(e)
                }
        
        return test_results
    
    def test_tag_task_data_loading(self):
        """测试标签任务数据加载"""
        logger.info("🏷️ 测试标签任务数据加载...")
        
        # 测试不同标签任务的数据加载
        tag_tasks_config = [
            {
                'tag_id': 1,
                'tag_name': '高净值用户',
                'data_source': 'user_asset_summary',
                'required_fields': ['user_id', 'total_asset_value', 'cash_balance']
            },
            {
                'tag_id': 2,
                'tag_name': 'VIP客户',
                'data_source': 'user_basic_info',
                'required_fields': ['user_id', 'user_level', 'kyc_status']
            },
            {
                'tag_id': 4,
                'tag_name': '活跃交易者',
                'data_source': 'user_activity_summary',
                'required_fields': ['user_id', 'trade_count_30d']
            }
        ]
        
        from src.batch.core.data_loader import BatchDataLoader
        data_loader = BatchDataLoader(self.spark, self.config)
        
        task_results = {}
        
        for task_config in tag_tasks_config:
            try:
                logger.info(f"📊 测试任务: {task_config['tag_name']}")
                
                # 加载任务数据
                df = data_loader.load_user_data(
                    task_config['data_source'],
                    task_config['required_fields']
                )
                
                if df is not None:
                    count = df.count()
                    logger.info(f"   ✅ 数据加载成功: {count:,} 条记录")
                    
                    # 验证字段
                    actual_columns = set(df.columns)
                    required_columns = set(task_config['required_fields'])
                    
                    if required_columns.issubset(actual_columns):
                        logger.info(f"   ✅ 字段验证通过: {task_config['required_fields']}")
                    else:
                        missing = required_columns - actual_columns
                        logger.warning(f"   ⚠️ 缺少字段: {missing}")
                    
                    task_results[task_config['tag_name']] = {
                        'success': True,
                        'count': count,
                        'fields_ok': required_columns.issubset(actual_columns)
                    }
                else:
                    logger.error(f"   ❌ 数据加载失败: 返回None")
                    task_results[task_config['tag_name']] = {
                        'success': False,
                        'error': 'DataFrame is None'
                    }
                    
            except Exception as e:
                logger.error(f"   ❌ 任务 {task_config['tag_name']} 测试失败: {e}")
                task_results[task_config['tag_name']] = {
                    'success': False,
                    'error': str(e)
                }
        
        return task_results
    
    def test_rule_application(self):
        """测试规则应用"""
        logger.info("⚖️ 测试规则应用...")
        
        try:
            # 加载资产数据测试高净值用户规则
            from src.batch.core.data_loader import BatchDataLoader
            data_loader = BatchDataLoader(self.spark, self.config)
            
            asset_df = data_loader.load_user_data('user_asset_summary', ['user_id', 'total_asset_value'])
            
            if asset_df is None:
                logger.error("❌ 无法加载资产数据")
                return False
            
            # 应用高净值用户规则 (total_asset_value >= 150000)
            from pyspark.sql.functions import col
            high_net_worth_users = asset_df.filter(col('total_asset_value') >= 150000)
            
            total_users = asset_df.count()
            high_net_worth_count = high_net_worth_users.count()
            
            logger.info(f"📊 规则应用结果:")
            logger.info(f"   总用户数: {total_users:,}")
            logger.info(f"   高净值用户数: {high_net_worth_count:,}")
            logger.info(f"   匹配率: {high_net_worth_count/total_users*100:.1f}%")
            
            if high_net_worth_count > 0:
                # 显示样本用户
                sample_users = high_net_worth_users.limit(5).collect()
                logger.info(f"   样本高净值用户:")
                for user in sample_users:
                    logger.info(f"     {user.user_id}: {user.total_asset_value:,.2f}")
                
                logger.info("✅ 规则应用测试通过")
                return True
            else:
                logger.warning("⚠️ 没有用户匹配高净值规则")
                return False
                
        except Exception as e:
            logger.error(f"❌ 规则应用测试失败: {e}")
            return False
    
    def test_full_tag_calculation(self):
        """测试完整标签计算流程"""
        logger.info("🎯 测试完整标签计算流程...")
        
        try:
            # 导入并运行标签计算
            from src.batch.orchestrator.batch_orchestrator import BatchOrchestrator
            
            orchestrator = BatchOrchestrator(self.config)
            
            # 测试指定用户的标签计算
            test_user_ids = [f"user_{i:06d}" for i in range(1, 11)]  # 前10个用户
            
            logger.info(f"🔄 为 {len(test_user_ids)} 个用户计算标签...")
            
            # 执行标签计算
            result = orchestrator.execute_user_tags(
                user_ids=test_user_ids,
                tag_ids=[1, 2, 3],  # 测试前3个标签
                mode='user-tags'
            )
            
            if result and result.get('success'):
                logger.info("✅ 完整标签计算测试通过")
                logger.info(f"   处理用户数: {result.get('total_users', 0)}")
                logger.info(f"   生成标签数: {result.get('total_tags', 0)}")
                return True
            else:
                logger.error(f"❌ 标签计算失败: {result}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 完整标签计算测试失败: {e}")
            return False
    
    def run_all_tests(self):
        """运行所有测试"""
        logger.info("🚀 开始Hive集成测试")
        logger.info("=" * 50)
        
        test_results = {}
        
        try:
            # 1. 测试S3连接
            test_results['s3_connectivity'] = self.test_s3_connectivity()
            
            # 2. 创建Spark会话
            self.create_spark_session()
            
            # 3. 测试Hive表读取
            test_results['hive_table_reading'] = self.test_hive_table_reading()
            
            # 4. 测试标签任务数据加载
            test_results['tag_task_loading'] = self.test_tag_task_data_loading()
            
            # 5. 测试规则应用
            test_results['rule_application'] = self.test_rule_application()
            
            # 6. 测试完整标签计算
            test_results['full_tag_calculation'] = self.test_full_tag_calculation()
            
        except Exception as e:
            logger.error(f"❌ 测试过程中发生异常: {e}")
            test_results['error'] = str(e)
        
        finally:
            if self.spark:
                self.spark.stop()
                logger.info("🛑 Spark会话已关闭")
        
        # 显示测试总结
        self.show_test_summary(test_results)
        
        return test_results
    
    def show_test_summary(self, results):
        """显示测试总结"""
        logger.info("\n" + "=" * 50)
        logger.info("📊 Hive集成测试总结")
        logger.info("=" * 50)
        
        total_tests = 0
        passed_tests = 0
        
        # S3连接测试
        if results.get('s3_connectivity'):
            logger.info("✅ S3连接测试: 通过")
            passed_tests += 1
        else:
            logger.info("❌ S3连接测试: 失败")
        total_tests += 1
        
        # Hive表读取测试
        hive_results = results.get('hive_table_reading', {})
        if hive_results:
            successful_tables = sum(1 for r in hive_results.values() if r.get('success'))
            total_tables = len(hive_results)
            if successful_tables == total_tables:
                logger.info(f"✅ Hive表读取测试: 通过 ({successful_tables}/{total_tables})")
                passed_tests += 1
            else:
                logger.info(f"⚠️ Hive表读取测试: 部分通过 ({successful_tables}/{total_tables})")
        else:
            logger.info("❌ Hive表读取测试: 失败")
        total_tests += 1
        
        # 标签任务数据加载测试
        task_results = results.get('tag_task_loading', {})
        if task_results:
            successful_tasks = sum(1 for r in task_results.values() if r.get('success'))
            total_tasks = len(task_results)
            if successful_tasks == total_tasks:
                logger.info(f"✅ 标签任务数据加载测试: 通过 ({successful_tasks}/{total_tasks})")
                passed_tests += 1
            else:
                logger.info(f"⚠️ 标签任务数据加载测试: 部分通过 ({successful_tasks}/{total_tasks})")
        else:
            logger.info("❌ 标签任务数据加载测试: 失败")
        total_tests += 1
        
        # 规则应用测试
        if results.get('rule_application'):
            logger.info("✅ 规则应用测试: 通过")
            passed_tests += 1
        else:
            logger.info("❌ 规则应用测试: 失败")
        total_tests += 1
        
        # 完整标签计算测试
        if results.get('full_tag_calculation'):
            logger.info("✅ 完整标签计算测试: 通过")
            passed_tests += 1
        else:
            logger.info("❌ 完整标签计算测试: 失败")
        total_tests += 1
        
        # 总结
        success_rate = passed_tests / total_tests * 100
        logger.info(f"\n🎯 测试总体结果: {passed_tests}/{total_tests} 通过 ({success_rate:.1f}%)")
        
        if success_rate >= 80:
            logger.info("🎉 Hive集成测试整体通过！系统可以正常使用真实的S3/Hive数据")
        elif success_rate >= 60:
            logger.info("⚠️ Hive集成测试部分通过，某些功能可能需要检查")
        else:
            logger.info("❌ Hive集成测试失败，请检查配置和数据")


def main():
    """主函数"""
    tester = HiveIntegrationTester()
    tester.run_all_tests()


if __name__ == "__main__":
    main()