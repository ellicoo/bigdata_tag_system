#!/usr/bin/env python3
"""
AWS Glue生产环境作业脚本
"""

import sys
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# 添加项目路径
import os
sys.path.append('/opt/ml/code')  # Glue作业代码路径

from src.config.manager import ConfigManager
from src.scheduler.tag_scheduler import TagScheduler


def setup_glue_logging(log_level="WARN"):
    """设置Glue日志 - 生产环境减少日志量"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)


def main():
    """Glue生产作业主函数"""
    
    # 解析Glue参数
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'mode',
        'days',
        'tag_ids',
        'log_level'
    ])
    
    # 设置日志 - 生产环境默认WARN级别
    logger = setup_glue_logging(args.get('log_level', 'WARN'))
    logger.info(f"🚀 启动Glue生产标签计算作业: {args['JOB_NAME']}")
    
    # 初始化Glue上下文
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    try:
        # 加载Glue生产环境配置
        config = ConfigManager.load_config('glue-prod')
        logger.info("✅ 生产环境配置加载完成")
        
        # 创建调度器
        scheduler = TagScheduler(config)
        
        # 初始化系统
        logger.info("📋 初始化生产标签计算系统...")
        scheduler.initialize()
        
        # 健康检查
        logger.info("🏥 执行生产系统健康检查...")
        if not scheduler.health_check():
            logger.error("❌ 生产系统健康检查失败")
            job.commit()
            return 1
        
        logger.info("✅ 生产系统健康检查通过")
        
        # 根据模式执行任务
        success = False
        mode = args['mode'].lower()
        
        if mode == 'full':
            logger.info("🎯 执行生产全量标签计算")
            success = scheduler.run_full_tag_compute()
            
        elif mode == 'incremental':
            days_back = int(args.get('days', '1'))
            logger.info(f"🎯 执行生产增量标签计算，回溯{days_back}天")
            success = scheduler.run_incremental_compute(days_back)
            
        elif mode == 'tags':
            tag_ids_str = args.get('tag_ids', '')
            if not tag_ids_str:
                logger.error("❌ 指定标签模式需要提供tag_ids参数")
                job.commit()
                return 1
            
            try:
                tag_ids = [int(x.strip()) for x in tag_ids_str.split(',')]
                logger.info(f"🎯 执行生产指定标签计算: {tag_ids}")
                success = scheduler.run_specific_tags(tag_ids)
            except ValueError:
                logger.error("❌ 标签ID格式错误，应为逗号分隔的数字")
                job.commit()
                return 1
        
        else:
            logger.error(f"❌ 不支持的执行模式: {mode}")
            job.commit()
            return 1
        
        if success:
            logger.info("🎉 生产标签计算任务执行成功")
            exit_code = 0
        else:
            logger.error("❌ 生产标签计算任务执行失败")
            exit_code = 1
            
    except Exception as e:
        logger.error(f"❌ 生产Glue作业执行异常: {str(e)}")
        exit_code = 1
        
    finally:
        # 清理资源
        try:
            if 'scheduler' in locals():
                scheduler.cleanup()
                logger.info("🧹 生产环境资源清理完成")
        except Exception as cleanup_error:
            logger.warning(f"⚠️ 生产环境资源清理异常: {cleanup_error}")
        
        # 提交Glue作业
        job.commit()
        logger.info("👋 生产Glue作业结束")
    
    return exit_code


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)