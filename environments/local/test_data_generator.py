"""
本地环境测试数据生成器
"""

import sys
import os

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.config.manager import ConfigManager


def _upload_parquet_to_s3(s3_client, bucket_name, local_path, s3_prefix):
    """将本地Parquet文件上传到S3"""
    import os
    
    for root, dirs, files in os.walk(local_path):
        for file in files:
            if file.endswith('.parquet'):
                local_file = os.path.join(root, file)
                # 保持Parquet的分区结构
                relative_path = os.path.relpath(local_file, local_path)
                s3_key = f"{s3_prefix}/{relative_path}"
                
                with open(local_file, 'rb') as f:
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=s3_key,
                        Body=f.read()
                    )


def generate_test_data():
    """生成生产环境模拟的测试数据"""
    print("🗄️ 生成生产环境模拟测试数据...")
    
    try:
        from pyspark.sql import SparkSession
        import pandas as pd
        import random
        from datetime import datetime, timedelta
        import boto3
        from botocore.exceptions import ClientError
        
        # 加载本地配置
        config = ConfigManager.load_config('local')
        print(f"✅ 配置加载成功: {config.environment}")
        
        # 初始化Spark (简化配置，先在本地生成再上传)
        spark = SparkSession.builder \
            .appName("TestDataGenerator") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        # 配置MinIO客户端
        s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            region_name='us-east-1'
        )
        
        # 创建存储桶
        bucket_name = 'test-data-lake'
        try:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"✅ 创建存储桶: {bucket_name}")
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                print(f"📁 存储桶已存在: {bucket_name}")
            else:
                print(f"⚠️ 创建存储桶失败: {e}")
        
        # 1. 生成用户基础信息表 (user_basic_info)
        print("📊 生成用户基础信息表...")
        users = []
        for i in range(1000):  # 生成1000个用户，更真实
            user_id = f"user_{i+1:06d}"
            
            users.append({
                'user_id': user_id,
                'age': random.randint(18, 65),
                'registration_date': (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d'),
                'user_level': random.choice(['BRONZE', 'SILVER', 'GOLD', 'VIP1', 'VIP2', 'VIP3']),
                'kyc_status': random.choice(['verified', 'pending', 'rejected']),
                'last_login_date': (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d'),
                'created_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        
        # 转换为Spark DataFrame并保存为本地Parquet，然后上传
        import tempfile
        import os
        
        user_basic_df = spark.createDataFrame(users)
        
        # 先保存到临时目录
        temp_dir = tempfile.mkdtemp()
        local_basic_path = os.path.join(temp_dir, "user_basic_info")
        user_basic_df.write.mode("overwrite").parquet(local_basic_path)
        
        # 上传到MinIO
        _upload_parquet_to_s3(s3_client, bucket_name, local_basic_path, "warehouse/user_basic_info")
        print(f"✅ 用户基础信息表已保存到: s3a://{bucket_name}/warehouse/user_basic_info")
        
        # 2. 生成用户资产汇总表 (user_asset_summary)  
        print("💰 生成用户资产汇总表...")
        assets = []
        for user in users:
            # 为部分用户生成高净值，模拟真实分布
            if random.random() < 0.3:  # 30%的用户是高净值
                total_asset = random.randint(100000, 2000000)
                cash_balance = random.randint(10000, 200000)
            else:
                total_asset = random.randint(1000, 99999)
                cash_balance = random.randint(100, 50000)
                
            assets.append({
                'user_id': user['user_id'],
                'total_asset_value': float(total_asset),
                'cash_balance': float(cash_balance),
                'stock_value': float(random.randint(0, total_asset // 2)),
                'bond_value': float(random.randint(0, total_asset // 4)),
                'fund_value': float(random.randint(0, total_asset // 3)),
                'computed_date': datetime.now().strftime('%Y-%m-%d'),
                'created_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        
        user_asset_df = spark.createDataFrame(assets)
        local_asset_path = os.path.join(temp_dir, "user_asset_summary")
        user_asset_df.write.mode("overwrite").parquet(local_asset_path)
        
        _upload_parquet_to_s3(s3_client, bucket_name, local_asset_path, "warehouse/user_asset_summary")
        print(f"✅ 用户资产汇总表已保存到: s3a://{bucket_name}/warehouse/user_asset_summary")
        
        # 3. 生成用户活动汇总表 (user_activity_summary)
        print("📈 生成用户活动汇总表...")
        activities = []
        for user in users:
            activities.append({
                'user_id': user['user_id'],
                'trade_count_30d': random.randint(0, 100),
                'trade_amount_30d': float(random.randint(0, 1000000)),
                'login_count_30d': random.randint(0, 30),
                'risk_score': random.randint(10, 90),
                'computed_date': datetime.now().strftime('%Y-%m-%d'),
                'created_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
        
        user_activity_df = spark.createDataFrame(activities)
        local_activity_path = os.path.join(temp_dir, "user_activity_summary")
        user_activity_df.write.mode("overwrite").parquet(local_activity_path)
        
        _upload_parquet_to_s3(s3_client, bucket_name, local_activity_path, "warehouse/user_activity_summary")
        print(f"✅ 用户活动汇总表已保存到: s3a://{bucket_name}/warehouse/user_activity_summary")
        
        # 清理临时文件
        import shutil
        shutil.rmtree(temp_dir)
        print("🧹 清理临时文件完成")
        
        # 统计信息
        total_users = len(users)
        high_value_users = len([a for a in assets if a['total_asset_value'] >= 100000])
        active_traders = len([a for a in activities if a['trade_count_30d'] > 10])
        low_risk_users = len([a for a in activities if a['risk_score'] <= 30])
        
        print("\n📊 生产环境模拟数据统计:")
        print(f"   - 总用户数: {total_users:,}")
        print(f"   - 高净值用户: {high_value_users:,} ({high_value_users/total_users*100:.1f}%)")
        print(f"   - 活跃交易者: {active_traders:,} ({active_traders/total_users*100:.1f}%)")
        print(f"   - 低风险用户: {low_risk_users:,} ({low_risk_users/total_users*100:.1f}%)")
        
        # VIP用户统计
        vip_users = len([u for u in users if u['user_level'] in ['VIP2', 'VIP3'] and u['kyc_status'] == 'verified'])
        print(f"   - VIP客户: {vip_users:,} ({vip_users/total_users*100:.1f}%)")
        
        print("\n🎯 数据已按生产环境标准生成:")
        print("   - 格式: Parquet (列式存储)")
        print("   - 存储: S3兼容的MinIO")
        print("   - 结构: 分表存储，模拟真实数仓")
        print("   - 规模: 1000用户，支持完整标签计算测试")
        
        spark.stop()
        
    except Exception as e:
        print(f"❌ 测试数据生成失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    generate_test_data()