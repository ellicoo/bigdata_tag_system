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
        choices=[
            'health', 'full', 'incremental', 'tags', 'users', 'user-tags', 'incremental-tags',
            'full-parallel', 'tags-parallel', 'incremental-parallel', 'incremental-tags-parallel', 'users-parallel', 'user-tags-parallel'
        ],
        required=True,
        help='''执行模式:
        health - 系统健康检查
        full - 全量计算（全量用户，全量标签）
        incremental - 增量计算（新增用户，全量标签）
        tags - 指定标签计算（全量用户，指定标签）
        users - 指定用户计算（指定用户，全量标签）
        user-tags - 指定用户指定标签计算（指定用户，指定标签）
        incremental-tags - 增量指定标签计算（新增用户，指定标签）
        
        --- 并行优化版本 ---
        full-parallel - 全量用户打全量标签（并行优化版，内存合并，不与MySQL合并）
        tags-parallel - 全量用户打指定标签（并行优化版，内存合并，与MySQL合并）
        incremental-parallel - 增量用户打全量标签（并行优化版，内存合并，不与MySQL合并）
        incremental-tags-parallel - 增量用户打指定标签（并行优化版，内存合并，不与MySQL合并）
        users-parallel - 指定用户打全量标签（并行优化版，内存合并，不与MySQL合并）
        user-tags-parallel - 指定用户打指定标签（并行优化版，内存合并，与MySQL合并）'''
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
        help='指定标签ID列表，逗号分隔 (例如: 1,2,3) - 用于tags、user-tags、incremental-tags模式'
    )
    
    # 指定用户参数
    parser.add_argument(
        '--user-ids',
        type=str,
        help='指定用户ID列表，逗号分隔 (例如: user_000001,user_000002) - 用于users和user-tags模式'
    )
    
    # 并行控制参数
    parser.add_argument(
        '--parallel',
        action='store_true',
        help='强制启用并行计算模式'
    )
    parser.add_argument(
        '--atomic',
        action='store_true', 
        help='强制启用原子写入模式'
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        default=4,
        help='最大并行工作线程数 (默认: 4)'
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
    
    if args.mode == 'users' and not args.user_ids:
        print("❌ 错误: --mode users 需要提供 --user-ids 参数")
        return False
    
    if args.mode == 'user-tags' and (not args.user_ids or not args.tag_ids):
        print("❌ 错误: --mode user-tags 需要同时提供 --user-ids 和 --tag-ids 参数")
        return False
    
    if args.mode == 'incremental-tags' and not args.tag_ids:
        print("❌ 错误: --mode incremental-tags 需要提供 --tag-ids 参数")
        return False
    
    if args.mode in ['incremental', 'incremental-parallel'] and args.days <= 0:
        print("❌ 错误: --days 必须大于0")
        return False
    
    # 并行优化版本验证
    if args.mode == 'tags-parallel' and not args.tag_ids:
        print("❌ 错误: --mode tags-parallel 需要提供 --tag-ids 参数")
        return False
    
    if args.mode == 'incremental-tags-parallel' and not args.tag_ids:
        print("❌ 错误: --mode incremental-tags-parallel 需要提供 --tag-ids 参数")
        return False
    
    if args.mode == 'users-parallel' and not args.user_ids:
        print("❌ 错误: --mode users-parallel 需要提供 --user-ids 参数")
        return False
    
    if args.mode == 'user-tags-parallel' and (not args.user_ids or not args.tag_ids):
        print("❌ 错误: --mode user-tags-parallel 需要同时提供 --user-ids 和 --tag-ids 参数")
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
        scheduler = TagComputeScheduler(
            config, 
            parallel_mode=args.parallel,
            atomic_mode=args.atomic,
            max_workers=args.max_workers
        )
        
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
                logger.info(f"🎯 执行指定标签计算（全量用户）: {tag_ids}")
                success = scheduler.run_specific_tags(tag_ids)
            except ValueError:
                logger.error("❌ 标签ID格式错误，应为逗号分隔的数字")
                sys.exit(1)
        
        elif args.mode == 'users':
            try:
                user_ids = [x.strip() for x in args.user_ids.split(',')]
                logger.info(f"🎯 执行指定用户计算（全量标签）: {user_ids}")
                success = scheduler.run_specific_users(user_ids)
            except Exception as e:
                logger.error(f"❌ 用户ID格式错误: {e}")
                sys.exit(1)
        
        elif args.mode == 'user-tags':
            try:
                tag_ids = [int(x.strip()) for x in args.tag_ids.split(',')]
                user_ids = [x.strip() for x in args.user_ids.split(',')]
                logger.info(f"🎯 执行指定用户指定标签计算: 用户{user_ids}, 标签{tag_ids}")
                success = scheduler.run_specific_user_tags(user_ids, tag_ids)
            except ValueError:
                logger.error("❌ 标签ID格式错误，应为逗号分隔的数字")
                sys.exit(1)
            except Exception as e:
                logger.error(f"❌ 参数格式错误: {e}")
                sys.exit(1)
        
        elif args.mode == 'incremental-tags':
            try:
                tag_ids = [int(x.strip()) for x in args.tag_ids.split(',')]
                logger.info(f"🎯 执行增量指定标签计算，回溯{args.days}天: 标签{tag_ids}")
                success = scheduler.run_incremental_specific_tags(args.days, tag_ids)
            except ValueError:
                logger.error("❌ 标签ID格式错误，应为逗号分隔的数字")
                sys.exit(1)
            except Exception as e:
                logger.error(f"❌ 参数格式错误: {e}")
                sys.exit(1)
        
        # ==================== 并行优化版本 ====================
        
        elif args.mode == 'full-parallel':
            logger.info("🎯 执行全量用户打全量标签（并行优化版）")
            success = scheduler.run_scenario_1_full_users_full_tags()
            
        elif args.mode == 'tags-parallel':
            try:
                tag_ids = [int(x.strip()) for x in args.tag_ids.split(',')]
                logger.info(f"🎯 执行全量用户打指定标签（并行优化版）: {tag_ids}")
                success = scheduler.run_scenario_2_full_users_specific_tags(tag_ids)
            except ValueError:
                logger.error("❌ 标签ID格式错误，应为逗号分隔的数字")
                sys.exit(1)
        
        elif args.mode == 'incremental-parallel':
            logger.info(f"🎯 执行增量用户打全量标签（并行优化版），回溯{args.days}天")
            success = scheduler.run_scenario_3_incremental_users_full_tags(args.days)
            
        elif args.mode == 'incremental-tags-parallel':
            try:
                tag_ids = [int(x.strip()) for x in args.tag_ids.split(',')]
                logger.info(f"🎯 执行增量用户打指定标签（并行优化版），回溯{args.days}天: {tag_ids}")
                success = scheduler.run_scenario_4_incremental_users_specific_tags(args.days, tag_ids)
            except ValueError:
                logger.error("❌ 标签ID格式错误，应为逗号分隔的数字")
                sys.exit(1)
        
        elif args.mode == 'users-parallel':
            try:
                user_ids = [x.strip() for x in args.user_ids.split(',')]
                logger.info(f"🎯 执行指定用户打全量标签（并行优化版）: {user_ids}")
                success = scheduler.run_scenario_5_specific_users_full_tags(user_ids)
            except Exception as e:
                logger.error(f"❌ 用户ID格式错误: {e}")
                sys.exit(1)
        
        elif args.mode == 'user-tags-parallel':
            try:
                tag_ids = [int(x.strip()) for x in args.tag_ids.split(',')]
                user_ids = [x.strip() for x in args.user_ids.split(',')]
                logger.info(f"🎯 执行指定用户打指定标签（并行优化版）: 用户{user_ids}, 标签{tag_ids}")
                success = scheduler.run_scenario_6_specific_users_specific_tags(user_ids, tag_ids)
            except ValueError:
                logger.error("❌ 标签ID格式错误，应为逗号分隔的数字")
                sys.exit(1)
            except Exception as e:
                logger.error(f"❌ 参数格式错误: {e}")
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