#!/usr/bin/env python3
"""
本地环境Hive表初始化脚本
创建真实的Hive表结构和业务数据
"""

import sys
import os
import logging
from datetime import datetime, timedelta
import random
from typing import Dict, List, Any
import tempfile
import shutil

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class HiveTableInitializer:
    """Hive表初始化器"""
    
    def __init__(self):
        """初始化配置"""
        from src.common.config.manager import ConfigManager
        import boto3
        from botocore.exceptions import ClientError
        
        self.config = ConfigManager.load_config('local')
        logger.info(f"✅ 配置加载成功: {self.config.environment}")
        
        # 配置MinIO客户端
        self.s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            region_name='us-east-1'
        )
        
        # 存储桶名称
        self.bucket_name = 'test-data-lake'
        self.warehouse_path = 'warehouse'
        
        # 创建临时目录
        self.temp_dir = tempfile.mkdtemp()
        logger.info(f"📁 临时目录: {self.temp_dir}")
    
    def ensure_bucket_exists(self):
        """确保S3存储桶存在"""
        try:
            self.s3_client.create_bucket(Bucket=self.bucket_name)
            logger.info(f"✅ 创建存储桶: {self.bucket_name}")
        except Exception as e:
            if 'BucketAlreadyOwnedByYou' in str(e):
                logger.info(f"📁 存储桶已存在: {self.bucket_name}")
            else:
                logger.warning(f"⚠️ 创建存储桶异常: {e}")
    
    def create_spark_session(self):
        """创建Spark会话"""
        from pyspark.sql import SparkSession
        
        # 获取JAR文件路径
        jars_dir = os.path.join(os.path.dirname(__file__), "jars")
        jar_files = [
            os.path.join(jars_dir, "mysql-connector-j-8.0.33.jar"),
            os.path.join(jars_dir, "hadoop-aws-3.3.4.jar"),
            os.path.join(jars_dir, "aws-java-sdk-bundle-1.11.1034.jar")
        ]
        existing_jars = [jar for jar in jar_files if os.path.exists(jar)]
        
        spark = SparkSession.builder \
            .appName("HiveTableInitializer") \
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
        return spark
    
    def generate_user_basic_info(self, spark, num_users: int = 2000) -> None:
        """生成用户基础信息表 - user_basic_info"""
        logger.info(f"📊 生成用户基础信息表 ({num_users} 用户)...")
        
        # 生成多样化的用户数据，确保标签规则能够匹配
        users = []
        
        for i in range(num_users):
            user_id = f"user_{i+1:06d}"
            
            # 年龄分布：确保有足够的年轻用户（18-30岁）
            if i < 400:  # 20%年轻用户
                age = random.randint(18, 30)
            elif i < 1200:  # 40%中年用户
                age = random.randint(31, 50)
            else:  # 40%老年用户
                age = random.randint(51, 65)
            
            # 用户等级分布：确保有VIP用户
            if i < 100:  # 5% VIP3
                user_level = 'VIP3'
            elif i < 250:  # 7.5% VIP2
                user_level = 'VIP2'
            elif i < 450:  # 10% VIP1
                user_level = 'VIP1'
            elif i < 750:  # 15% GOLD
                user_level = 'GOLD'
            elif i < 1250:  # 25% SILVER
                user_level = 'SILVER'
            else:  # 37.5% BRONZE
                user_level = 'BRONZE'
            
            # KYC状态：VIP用户优先verified
            if user_level in ['VIP2', 'VIP3']:
                kyc_status = 'verified'
            else:
                kyc_status = random.choices(
                    ['verified', 'pending', 'rejected'],
                    weights=[0.7, 0.2, 0.1]
                )[0]
            
            # 注册日期：确保有新用户（最近30天）
            if i < 300:  # 15%新用户
                reg_days_ago = random.randint(1, 30)
            else:
                reg_days_ago = random.randint(31, 365)
            
            registration_date = (datetime.now() - timedelta(days=reg_days_ago)).strftime('%Y-%m-%d')
            
            # 最后登录：确保有最近活跃用户
            if i < 400:  # 20%最近活跃
                login_days_ago = random.randint(0, 7)
            else:
                login_days_ago = random.randint(8, 30)
            
            last_login_date = (datetime.now() - timedelta(days=login_days_ago)).strftime('%Y-%m-%d')
            
            users.append({
                'user_id': user_id,
                'age': age,
                'gender': random.choice(['M', 'F']),
                'registration_date': registration_date,
                'user_level': user_level,
                'kyc_status': kyc_status,
                'last_login_date': last_login_date,
                'country': random.choice(['CN', 'US', 'SG', 'HK']),
                'city': random.choice(['Beijing', 'Shanghai', 'Shenzhen', 'Singapore', 'HongKong']),
                'created_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'updated_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        
        # 创建DataFrame并保存
        user_basic_df = spark.createDataFrame(users)
        local_path = os.path.join(self.temp_dir, "user_basic_info")
        user_basic_df.write.mode("overwrite").parquet(local_path)
        
        # 上传到S3
        self._upload_to_s3(local_path, f"{self.warehouse_path}/user_basic_info")
        
        # 统计信息
        young_users = len([u for u in users if u['age'] <= 30])
        vip_users = len([u for u in users if u['user_level'] in ['VIP2', 'VIP3'] and u['kyc_status'] == 'verified'])
        new_users = len([u for u in users if (datetime.now() - datetime.strptime(u['registration_date'], '%Y-%m-%d')).days <= 30])
        recent_active = len([u for u in users if (datetime.now() - datetime.strptime(u['last_login_date'], '%Y-%m-%d')).days <= 7])
        
        logger.info(f"✅ user_basic_info 创建完成:")
        logger.info(f"   - 总用户数: {num_users:,}")
        logger.info(f"   - 年轻用户 (≤30岁): {young_users:,} ({young_users/num_users*100:.1f}%)")
        logger.info(f"   - VIP客户 (VIP2/3+验证): {vip_users:,} ({vip_users/num_users*100:.1f}%)")
        logger.info(f"   - 新用户 (30天内): {new_users:,} ({new_users/num_users*100:.1f}%)")
        logger.info(f"   - 最近活跃 (7天内): {recent_active:,} ({recent_active/num_users*100:.1f}%)")
    
    def generate_user_asset_summary(self, spark, users_data: List[Dict]) -> None:
        """生成用户资产汇总表 - user_asset_summary"""
        logger.info("💰 生成用户资产汇总表...")
        
        assets = []
        for user in users_data:
            user_id = user['user_id']
            user_level = user['user_level']
            
            # 根据用户等级生成不同的资产分布
            if user_level == 'VIP3':
                # VIP3：高净值用户
                total_asset = random.randint(500000, 2000000)
                cash_balance = random.randint(100000, 500000)
            elif user_level == 'VIP2':
                # VIP2：中高净值用户
                total_asset = random.randint(200000, 800000)
                cash_balance = random.randint(50000, 200000)
            elif user_level == 'VIP1':
                # VIP1：有一定资产
                total_asset = random.randint(100000, 300000)
                cash_balance = random.randint(30000, 100000)
            elif user_level == 'GOLD':
                # GOLD：中等资产
                total_asset = random.randint(50000, 150000)
                cash_balance = random.randint(15000, 60000)
            elif user_level == 'SILVER':
                # SILVER：小额资产
                total_asset = random.randint(10000, 80000)
                cash_balance = random.randint(3000, 30000)
            else:  # BRONZE
                # BRONZE：新手用户
                total_asset = random.randint(1000, 50000)
                cash_balance = random.randint(500, 15000)
            
            # 其他资产类型分配
            stock_value = random.randint(0, int(total_asset * 0.6))
            bond_value = random.randint(0, int(total_asset * 0.3))
            fund_value = max(0, total_asset - cash_balance - stock_value - bond_value)
            
            assets.append({
                'user_id': user_id,
                'total_asset_value': float(total_asset),
                'cash_balance': float(cash_balance),
                'stock_value': float(stock_value),
                'bond_value': float(bond_value),
                'fund_value': float(fund_value),
                'crypto_value': float(random.randint(0, int(total_asset * 0.1))),
                'computed_date': datetime.now().strftime('%Y-%m-%d'),
                'created_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'updated_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        
        # 创建DataFrame并保存
        user_asset_df = spark.createDataFrame(assets)
        local_path = os.path.join(self.temp_dir, "user_asset_summary")
        user_asset_df.write.mode("overwrite").parquet(local_path)
        
        # 上传到S3
        self._upload_to_s3(local_path, f"{self.warehouse_path}/user_asset_summary")
        
        # 统计信息
        high_net_worth = len([a for a in assets if a['total_asset_value'] >= 150000])
        cash_rich = len([a for a in assets if a['cash_balance'] >= 60000])
        
        logger.info(f"✅ user_asset_summary 创建完成:")
        logger.info(f"   - 高净值用户 (≥150K): {high_net_worth:,} ({high_net_worth/len(assets)*100:.1f}%)")
        logger.info(f"   - 现金富裕用户 (≥60K): {cash_rich:,} ({cash_rich/len(assets)*100:.1f}%)")
    
    def generate_user_activity_summary(self, spark, users_data: List[Dict]) -> None:
        """生成用户活动汇总表 - user_activity_summary"""
        logger.info("📈 生成用户活动汇总表...")
        
        activities = []
        for i, user in enumerate(users_data):
            user_id = user['user_id']
            user_level = user['user_level']
            
            # 根据用户等级生成不同的活动模式
            if user_level in ['VIP3', 'VIP2']:
                # VIP用户：更活跃的交易
                trade_count_30d = random.randint(15, 100)
                trade_amount_30d = random.randint(50000, 1000000)
                login_count_30d = random.randint(20, 30)
                # VIP用户风险评分相对较低
                risk_score = random.randint(10, 40)
            elif user_level in ['VIP1', 'GOLD']:
                # 中级用户：中等活跃度
                trade_count_30d = random.randint(8, 50)
                trade_amount_30d = random.randint(10000, 200000)
                login_count_30d = random.randint(10, 25)
                risk_score = random.randint(15, 60)
            else:
                # 普通用户：较低活跃度
                trade_count_30d = random.randint(0, 20)
                trade_amount_30d = random.randint(0, 50000)
                login_count_30d = random.randint(1, 15)
                risk_score = random.randint(20, 80)
            
            # 确保有足够的活跃交易者（trade_count_30d > 15）
            if i < 300:  # 前300个用户强制为活跃交易者
                trade_count_30d = max(trade_count_30d, 16)
            
            # 确保有足够的低风险用户（risk_score <= 30）
            if i < 400:  # 前400个用户有较低风险
                risk_score = min(risk_score, 30)
            
            # 生成最近登录时间
            base_date = datetime.now()
            
            # 确保有足够的最近活跃用户（最近7天内登录）
            if i < 200:  # 前200个用户为最近活跃用户
                last_login_date = base_date - timedelta(days=random.randint(1, 7))
            elif user_level in ['VIP3', 'VIP2']:
                # VIP用户相对更活跃
                last_login_date = base_date - timedelta(days=random.randint(1, 15))
            else:
                # 普通用户登录频率较低
                last_login_date = base_date - timedelta(days=random.randint(1, 90))
            
            activities.append({
                'user_id': user_id,
                'trade_count_30d': trade_count_30d,
                'trade_amount_30d': float(trade_amount_30d),
                'login_count_30d': login_count_30d,
                'last_login_date': last_login_date.strftime('%Y-%m-%d'),  # 添加最近登录日期
                'page_view_count_30d': random.randint(10, 500),
                'app_duration_minutes_30d': random.randint(60, 3000),
                'risk_score': float(risk_score),
                'credit_score': float(random.randint(300, 850)),
                'computed_date': datetime.now().strftime('%Y-%m-%d'),
                'created_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'updated_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        
        # 创建DataFrame并保存
        user_activity_df = spark.createDataFrame(activities)
        local_path = os.path.join(self.temp_dir, "user_activity_summary")
        user_activity_df.write.mode("overwrite").parquet(local_path)
        
        # 上传到S3
        self._upload_to_s3(local_path, f"{self.warehouse_path}/user_activity_summary")
        
        # 统计信息
        active_traders = len([a for a in activities if a['trade_count_30d'] > 15])
        low_risk_users = len([a for a in activities if a['risk_score'] <= 30])
        # 统计最近活跃用户（最近7天内登录）
        recent_active_users = len([a for a in activities if (datetime.now() - datetime.strptime(a['last_login_date'], '%Y-%m-%d')).days <= 7])
        
        logger.info(f"✅ user_activity_summary 创建完成:")
        logger.info(f"   - 活跃交易者 (>15次/月): {active_traders:,} ({active_traders/len(activities)*100:.1f}%)")
        logger.info(f"   - 低风险用户 (≤30分): {low_risk_users:,} ({low_risk_users/len(activities)*100:.1f}%)")
        logger.info(f"   - 最近活跃用户 (7天内登录): {recent_active_users:,} ({recent_active_users/len(activities)*100:.1f}%)")
    
    def generate_user_transaction_detail(self, spark, users_data: List[Dict]) -> None:
        """生成用户交易明细表 - user_transaction_detail（可选）"""
        logger.info("💳 生成用户交易明细表...")
        
        transactions = []
        
        # 为前500个用户生成详细交易记录
        for user in users_data[:500]:
            user_id = user['user_id']
            
            # 每个用户生成5-20笔交易
            tx_count = random.randint(5, 20)
            
            for j in range(tx_count):
                tx_date = datetime.now() - timedelta(days=random.randint(1, 90))
                
                transactions.append({
                    'user_id': user_id,
                    'transaction_id': f"TX_{user_id}_{j+1:03d}",
                    'transaction_type': random.choice(['BUY', 'SELL', 'DEPOSIT', 'WITHDRAW']),
                    'product_type': random.choice(['STOCK', 'BOND', 'FUND', 'CRYPTO']),
                    'amount': float(random.randint(100, 50000)),
                    'transaction_date': tx_date.strftime('%Y-%m-%d'),
                    'transaction_time': tx_date.strftime('%H:%M:%S'),
                    'status': random.choice(['SUCCESS', 'PENDING', 'FAILED']),
                    'created_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                })
        
        # 创建DataFrame并保存
        if transactions:
            user_tx_df = spark.createDataFrame(transactions)
            local_path = os.path.join(self.temp_dir, "user_transaction_detail")
            user_tx_df.write.mode("overwrite").parquet(local_path)
            
            # 上传到S3
            self._upload_to_s3(local_path, f"{self.warehouse_path}/user_transaction_detail")
            
            logger.info(f"✅ user_transaction_detail 创建完成: {len(transactions):,} 笔交易")
    
    def _upload_to_s3(self, local_path: str, s3_prefix: str):
        """将本地Parquet文件上传到S3"""
        for root, dirs, files in os.walk(local_path):
            for file in files:
                if file.endswith('.parquet'):
                    local_file = os.path.join(root, file)
                    # 保持Parquet的分区结构
                    relative_path = os.path.relpath(local_file, local_path)
                    s3_key = f"{s3_prefix}/{relative_path}"
                    
                    with open(local_file, 'rb') as f:
                        self.s3_client.put_object(
                            Bucket=self.bucket_name,
                            Key=s3_key,
                            Body=f.read()
                        )
        
        logger.info(f"📤 已上传到: s3a://{self.bucket_name}/{s3_prefix}")
    
    def create_hive_metadata(self, spark):
        """创建Hive表元数据（可选，用于支持SQL查询）"""
        logger.info("📋 创建Hive表元数据...")
        
        try:
            # 注册临时视图，模拟Hive表
            
            # 1. user_basic_info
            basic_df = spark.read.parquet(f"s3a://{self.bucket_name}/{self.warehouse_path}/user_basic_info")
            basic_df.createOrReplaceTempView("user_basic_info")
            
            # 2. user_asset_summary
            asset_df = spark.read.parquet(f"s3a://{self.bucket_name}/{self.warehouse_path}/user_asset_summary")
            asset_df.createOrReplaceTempView("user_asset_summary")
            
            # 3. user_activity_summary
            activity_df = spark.read.parquet(f"s3a://{self.bucket_name}/{self.warehouse_path}/user_activity_summary")
            activity_df.createOrReplaceTempView("user_activity_summary")
            
            # 验证表创建
            tables = spark.catalog.listTables()
            table_names = [t.name for t in tables]
            
            logger.info(f"✅ Hive表元数据创建完成:")
            for table in table_names:
                count = spark.table(table).count()
                logger.info(f"   - {table}: {count:,} 条记录")
                
        except Exception as e:
            logger.warning(f"⚠️ Hive表元数据创建失败: {e}")
    
    def validate_data_quality(self, spark):
        """验证数据质量和标签规则匹配度"""
        logger.info("🔍 验证数据质量...")
        
        try:
            # 验证每个标签的数据匹配情况
            validations = []
            
            # 1. 高净值用户 (total_asset_value >= 150000)
            asset_df = spark.read.parquet(f"s3a://{self.bucket_name}/{self.warehouse_path}/user_asset_summary")
            high_net_worth = asset_df.filter(asset_df.total_asset_value >= 150000).count()
            validations.append(("高净值用户", high_net_worth))
            
            # 2. VIP用户 (user_level in ['VIP2', 'VIP3'] AND kyc_status = 'verified')
            basic_df = spark.read.parquet(f"s3a://{self.bucket_name}/{self.warehouse_path}/user_basic_info")
            vip_users = basic_df.filter(
                (basic_df.user_level.isin(['VIP2', 'VIP3'])) & 
                (basic_df.kyc_status == 'verified')
            ).count()
            validations.append(("VIP客户", vip_users))
            
            # 3. 年轻用户 (age <= 30)
            young_users = basic_df.filter(basic_df.age <= 30).count()
            validations.append(("年轻用户", young_users))
            
            # 4. 活跃交易者 (trade_count_30d > 15)
            activity_df = spark.read.parquet(f"s3a://{self.bucket_name}/{self.warehouse_path}/user_activity_summary")
            active_traders = activity_df.filter(activity_df.trade_count_30d > 15).count()
            validations.append(("活跃交易者", active_traders))
            
            # 5. 低风险用户 (risk_score <= 30)
            low_risk = activity_df.filter(activity_df.risk_score <= 30).count()
            validations.append(("低风险用户", low_risk))
            
            # 6. 新用户 (registration_date >= 30天前)
            from pyspark.sql.functions import col, date_sub, current_date
            new_users = basic_df.filter(
                col('registration_date') >= date_sub(current_date(), 30)
            ).count()
            validations.append(("新用户", new_users))
            
            # 7. 现金富裕用户 (cash_balance >= 60000)
            cash_rich = asset_df.filter(asset_df.cash_balance >= 60000).count()
            validations.append(("现金富裕用户", cash_rich))
            
            # 8. 最近活跃用户 (last_login_date >= 7天前)
            recent_active = activity_df.filter(
                col('last_login_date') >= date_sub(current_date(), 7)
            ).count()
            validations.append(("最近活跃用户", recent_active))
            
            logger.info("✅ 数据质量验证结果:")
            for label, count in validations:
                logger.info(f"   - {label}: {count:,} 用户符合条件")
                
            # 检查是否所有标签都有足够的用户
            min_users = min(count for _, count in validations)
            if min_users < 10:
                logger.warning(f"⚠️ 某些标签匹配用户数较少，最少: {min_users}")
            else:
                logger.info(f"✅ 所有标签都有足够的匹配用户，最少: {min_users}")
                
        except Exception as e:
            logger.error(f"❌ 数据质量验证失败: {e}")
    
    def cleanup(self):
        """清理临时文件"""
        try:
            shutil.rmtree(self.temp_dir)
            logger.info("🧹 临时文件清理完成")
        except Exception as e:
            logger.warning(f"⚠️ 临时文件清理失败: {e}")
    
    def initialize_all_tables(self):
        """初始化所有Hive表"""
        try:
            logger.info("🚀 开始初始化Hive表结构...")
            
            # 1. 确保存储桶存在
            self.ensure_bucket_exists()
            
            # 2. 创建Spark会话
            spark = self.create_spark_session()
            
            # 3. 生成用户基础信息表
            self.generate_user_basic_info(spark, num_users=2000)
            
            # 4. 获取用户数据用于关联表生成
            # 注意：由于Spark S3集成问题，这里使用生成的数据而不是从S3读取
            logger.info("📊 生成关联表数据...")
            users_data = []
            for i in range(2000):
                user_id = f"user_{i+1:06d}"
                if i < 400:  # 20%年轻用户
                    age = random.randint(18, 30)
                elif i < 1200:  # 40%中年用户
                    age = random.randint(31, 50)
                else:  # 40%老年用户
                    age = random.randint(51, 65)
                
                if i < 100:  # 5% VIP3
                    user_level = 'VIP3'
                elif i < 250:  # 7.5% VIP2
                    user_level = 'VIP2'
                elif i < 450:  # 10% VIP1
                    user_level = 'VIP1'
                elif i < 750:  # 15% GOLD
                    user_level = 'GOLD'
                elif i < 1250:  # 25% SILVER
                    user_level = 'SILVER'
                else:  # 37.5% BRONZE
                    user_level = 'BRONZE'
                
                users_data.append({
                    'user_id': user_id,
                    'user_level': user_level,
                    'age': age
                })
            
            # 5. 生成资产汇总表
            self.generate_user_asset_summary(spark, users_data)
            
            # 6. 生成活动汇总表
            self.generate_user_activity_summary(spark, users_data)
            
            # 7. 生成交易明细表（可选）
            self.generate_user_transaction_detail(spark, users_data)
            
            # 8. 创建Hive表元数据
            self.create_hive_metadata(spark)
            
            # 9. 验证数据质量
            self.validate_data_quality(spark)
            
            logger.info("🎉 Hive表初始化完成！")
            logger.info("================================")
            logger.info("📊 已创建的表:")
            logger.info("  ✅ user_basic_info - 用户基础信息")
            logger.info("  ✅ user_asset_summary - 用户资产汇总")
            logger.info("  ✅ user_activity_summary - 用户活动汇总")
            logger.info("  ✅ user_transaction_detail - 用户交易明细")
            logger.info("")
            logger.info("🎯 现在可以运行真实的S3/Hive数据读取测试:")
            logger.info("  cd ../../")
            logger.info("  python main.py --env local --mode health")
            logger.info("  python main.py --env local --mode full-parallel")
            
            spark.stop()
            
        except Exception as e:
            logger.error(f"❌ Hive表初始化失败: {e}")
            raise
        finally:
            self.cleanup()


def main():
    """主函数"""
    initializer = HiveTableInitializer()
    initializer.initialize_all_tables()


if __name__ == "__main__":
    main()