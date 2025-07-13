"""
本地环境测试数据生成器
"""

import sys
import os

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.config.manager import ConfigManager


def generate_test_data():
    """生成测试数据"""
    print("🗄️ 生成测试数据...")
    
    try:
        # 加载本地配置
        config = ConfigManager.load_config('local')
        print(f"✅ 配置加载成功: {config.environment}")
        
        # TODO: 实现测试数据生成逻辑
        # 1. 连接MySQL，创建示例标签规则
        # 2. 在MinIO中创建示例S3数据
        # 3. 生成示例用户数据
        
        print("⚠️ 测试数据生成器待实现")
        print("📋 当前可用功能:")
        print("   - 健康检查: python main.py --env local --mode health")
        print("   - Spark Web UI: http://localhost:8080")
        print("   - MinIO Console: http://localhost:9001")
        print("   - MySQL连接: localhost:3307")
        
    except Exception as e:
        print(f"❌ 测试数据生成失败: {e}")


if __name__ == "__main__":
    generate_test_data()