#!/usr/bin/env python3
"""
AWS Glue生产环境部署脚本
"""

import os
import sys
import boto3
import zipfile
import tempfile
from pathlib import Path


class GlueProdDeployer:
    """Glue生产环境部署器"""
    
    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.glue_client = boto3.client('glue')
        self.s3_client = boto3.client('s3')
        
        # 生产环境配置
        self.job_name = "tag-compute-prod"
        self.role_arn = os.getenv("PROD_GLUE_ROLE_ARN", "arn:aws:iam::ACCOUNT:role/GlueServiceRole-prod")
        self.s3_bucket = os.getenv("PROD_S3_BUCKET", "tag-system-prod-scripts")
        self.s3_key = "glue-jobs/tag-compute-prod.zip"
        
    def package_code(self):
        """打包项目代码"""
        print("📦 打包生产项目代码...")
        
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp_file:
            with zipfile.ZipFile(tmp_file.name, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                
                # 添加src目录
                src_dir = self.project_root / "src"
                for file_path in src_dir.rglob("*.py"):
                    arc_name = file_path.relative_to(self.project_root)
                    zip_file.write(file_path, arc_name)
                
                # 添加Glue作业脚本
                glue_job_path = Path(__file__).parent / "glue_job.py"
                zip_file.write(glue_job_path, "glue_job.py")
                
            return tmp_file.name
    
    def upload_to_s3(self, zip_path):
        """上传代码包到S3"""
        print(f"📤 上传生产代码包到S3: s3://{self.s3_bucket}/{self.s3_key}")
        
        self.s3_client.upload_file(zip_path, self.s3_bucket, self.s3_key)
        return f"s3://{self.s3_bucket}/{self.s3_key}"
    
    def create_or_update_job(self, script_location):
        """创建或更新Glue生产作业"""
        print(f"🔧 创建/更新生产Glue作业: {self.job_name}")
        
        job_definition = {
            'Name': self.job_name,
            'Description': '大数据标签系统 - 生产环境',
            'Role': self.role_arn,
            'Command': {
                'Name': 'glueetl',
                'ScriptLocation': script_location,
                'PythonVersion': '3'
            },
            'DefaultArguments': {
                '--additional-python-modules': 'pymysql,boto3',
                '--enable-metrics': '',
                '--enable-continuous-cloudwatch-log': 'true',
                '--enable-spark-ui': 'true',
                '--job-language': 'python',
                '--TempDir': f's3://{self.s3_bucket}/temp/',
                '--enable-auto-scaling': 'true',
            },
            'MaxRetries': 3,  # 生产环境更多重试
            'Timeout': 2880,  # 48小时
            'GlueVersion': '4.0',
            'WorkerType': 'G.1X',
            'NumberOfWorkers': 10,  # 生产环境更多Worker
            'SecurityConfiguration': os.getenv('PROD_SECURITY_CONFIG', ''),  # 生产安全配置
        }
        
        # 移除空的安全配置
        if not job_definition['SecurityConfiguration']:
            del job_definition['SecurityConfiguration']
        
        try:
            # 尝试获取现有作业
            self.glue_client.get_job(JobName=self.job_name)
            # 如果存在，则更新
            self.glue_client.update_job(JobName=self.job_name, JobUpdate=job_definition)
            print(f"✅ 生产作业 {self.job_name} 更新成功")
        except self.glue_client.exceptions.EntityNotFoundException:
            # 如果不存在，则创建
            self.glue_client.create_job(**job_definition)
            print(f"✅ 生产作业 {self.job_name} 创建成功")
    
    def validate_production_readiness(self):
        """验证生产环境就绪状态"""
        print("🔍 验证生产环境就绪状态...")
        
        required_env_vars = [
            'PROD_GLUE_ROLE_ARN',
            'PROD_S3_BUCKET',
            'PROD_MYSQL_HOST',
            'PROD_MYSQL_PASSWORD'
        ]
        
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        
        if missing_vars:
            print(f"❌ 缺少必需的生产环境变量: {', '.join(missing_vars)}")
            print("请设置这些环境变量后再部署到生产环境")
            return False
        
        print("✅ 生产环境配置验证通过")
        return True
    
    def deploy(self):
        """执行生产部署"""
        print("🚀 开始部署到Glue生产环境...")
        
        # 验证生产环境就绪状态
        if not self.validate_production_readiness():
            sys.exit(1)
        
        # 确认生产部署
        confirm = input("⚠️  这是生产环境部署，请确认 (yes/no): ")
        if confirm.lower() != 'yes':
            print("❌ 用户取消生产部署")
            sys.exit(1)
        
        try:
            # 1. 打包代码
            zip_path = self.package_code()
            
            # 2. 上传到S3
            script_location = self.upload_to_s3(zip_path)
            
            # 3. 创建/更新Glue作业
            self.create_or_update_job(script_location)
            
            # 4. 清理临时文件
            os.unlink(zip_path)
            
            print("🎉 生产环境部署完成！")
            print(f"📋 作业名称: {self.job_name}")
            print(f"📍 脚本位置: {script_location}")
            print("\n🚀 运行生产作业:")
            print(f"aws glue start-job-run --job-name {self.job_name} --arguments='--mode=health'")
            print("\n⚠️  请确保在CloudWatch中监控作业执行状态")
            
        except Exception as e:
            print(f"❌ 生产部署失败: {str(e)}")
            sys.exit(1)


def main():
    """主函数"""
    deployer = GlueProdDeployer()
    deployer.deploy()


if __name__ == "__main__":
    main()