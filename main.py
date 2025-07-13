#!/usr/bin/env python3
"""
大数据标签系统 - 统一入口
支持多环境：local, glue-dev, glue-prod
"""

import sys
import os
import argparse
import logging
from typing import Optional

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.config.manager import ConfigManager
from src.scheduler.main_scheduler import TagComputeScheduler


def setup_logging(log_level: str = "INFO"):
    """设置日志"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='大数据标签系统')
    
    # 环境配置
    parser.add_argument(
        '--env', '--environment',
        choices=['local', 'glue-dev', 'glue-prod'],
        default=os.getenv('TAG_SYSTEM_ENV', 'local'),
        help='运行环境 (默认: local)'
    )
    
    # 执行模式
    parser.add_argument(
        '--mode',
        choices=['health', 'full', 'incremental', 'tags'],
        required=True,
        help='执行模式: health(健康检查), full(全量), incremental(增量), tags(指定标签)'
    )
    
    # 增量计算参数
    parser.add_argument(
        '--days',
        type=int,
        default=1,
        help='增量计算回溯天数 (默认: 1)'
    )
    
    # 指定标签参数
    parser.add_argument(
        '--tag-ids',
        type=str,
        help='指定标签ID列表，逗号分隔 (例如: 1,2,3)'
    )
    
    # 日志级别
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'],
        help='日志级别'
    )
    
    return parser.parse_args()


def validate_arguments(args) -> bool:
    """验证参数"""
    if args.mode == 'tags' and not args.tag_ids:
        print("❌ 错误: --mode tags 需要提供 --tag-ids 参数")
        return False
    
    if args.mode == 'incremental' and args.days <= 0:
        print("❌ 错误: --days 必须大于0")
        return False
    
    return True


def main():
    """主函数"""
    args = parse_arguments()
    
    # 验证参数
    if not validate_arguments(args):
        sys.exit(1)
    
    # 加载配置
    try:
        config = ConfigManager.load_config(args.env)
        
        # 设置日志级别
        log_level = args.log_level or config.log_level
        logger = setup_logging(log_level)
        
        logger.info(f"🚀 启动标签系统")
        logger.info(f"📋 环境: {config.environment}")
        logger.info(f"🎯 模式: {args.mode}")
        logger.info(f"=" * 60)
        
    except Exception as e:
        print(f"❌ 配置加载失败: {e}")
        sys.exit(1)
    
    # 创建调度器
    try:
        scheduler = TagComputeScheduler(config)
        
        # 初始化系统
        logger.info("📋 初始化标签计算系统...")
        scheduler.initialize()
        
        # 执行任务
        success = False
        
        if args.mode == 'health':
            logger.info("🏥 执行系统健康检查...")
            success = scheduler.health_check()
            
        elif args.mode == 'full':
            logger.info("🎯 执行全量标签计算...")
            success = scheduler.run_full_tag_compute()
            
        elif args.mode == 'incremental':
            logger.info(f"🎯 执行增量标签计算，回溯{args.days}天...")
            success = scheduler.run_incremental_compute(args.days)
            
        elif args.mode == 'tags':
            try:
                tag_ids = [int(x.strip()) for x in args.tag_ids.split(',')]
                logger.info(f"🎯 执行指定标签计算: {tag_ids}")
                success = scheduler.run_specific_tags(tag_ids)
            except ValueError:
                logger.error("❌ 标签ID格式错误，应为逗号分隔的数字")
                sys.exit(1)
        
        # 输出结果
        if success:
            logger.info("=" * 60)
            logger.info("🎉 任务执行成功！")
            exit_code = 0
        else:
            logger.error("=" * 60)
            logger.error("❌ 任务执行失败！")
            exit_code = 1
            
    except Exception as e:
        logger.error(f"❌ 系统异常: {str(e)}")
        exit_code = 1
        
    finally:
        # 清理资源
        try:
            if 'scheduler' in locals():
                scheduler.cleanup()
                logger.info("🧹 资源清理完成")
        except Exception as e:
            logger.warning(f"⚠️ 资源清理异常: {e}")
    
    logger.info("👋 系统退出")
    sys.exit(exit_code)


if __name__ == "__main__":
    main()