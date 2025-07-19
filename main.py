#!/usr/bin/env python3
"""
大数据标签系统 - 统一入口
支持多环境：local, glue-dev, glue-prod
"""

import sys
import os
import argparse
import logging

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.common.config.manager import ConfigManager
from src.batch.orchestrator.batch_orchestrator import BatchOrchestrator


def setup_logging(log_level: str = "INFO"):
    """设置日志"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # 禁用其他第三方库的详细日志
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)
    logging.getLogger("py4j.clientserver").setLevel(logging.ERROR)
    
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
            'health', 'task-all', 'task-tags', 'task-users', 'list-tasks'
        ],
        required=True,
        help='''执行模式:
        health - 系统健康检查
        task-all - 任务化全量用户全量标签计算（执行所有已注册的任务类）
        task-tags - 任务化全量用户指定标签计算（执行指定标签对应的任务类）
        task-users - 任务化指定用户指定标签计算（执行指定用户指定标签的任务类）
        list-tasks - 列出所有可用的标签任务'''
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
    if args.mode == 'task-tags' and not args.tag_ids:
        print("❌ 错误: --mode task-tags 需要提供 --tag-ids 参数")
        return False
    
    if args.mode == 'task-users' and (not args.user_ids or not args.tag_ids):
        print("❌ 错误: --mode task-users 需要同时提供 --user-ids 和 --tag-ids 参数")
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
    
    # 创建批处理编排器
    try:
        scheduler = BatchOrchestrator(
            config=config,
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
            
        elif args.mode == 'task-tags':
            try:
                tag_ids = [int(x.strip()) for x in args.tag_ids.split(',')]
                logger.info(f"🎯 执行任务化全量用户指定标签计算: {tag_ids}")
                success = scheduler.execute_specific_tags_workflow(tag_ids)
            except ValueError:
                logger.error("❌ 标签ID格式错误，应为逗号分隔的数字")
                sys.exit(1)
        
        elif args.mode == 'task-users':
            try:
                tag_ids = [int(x.strip()) for x in args.tag_ids.split(',')]
                user_ids = [x.strip() for x in args.user_ids.split(',')]
                logger.info(f"🎯 执行任务化指定用户指定标签计算: 用户{user_ids}, 标签{tag_ids}")
                success = scheduler.execute_specific_users_workflow(user_ids, tag_ids)
            except ValueError:
                logger.error("❌ 标签ID格式错误，应为逗号分隔的数字")
                sys.exit(1)
            except Exception as e:
                logger.error(f"❌ 参数格式错误: {e}")
                sys.exit(1)
        
        elif args.mode == 'task-all':
            try:
                user_filter = None
                if args.user_ids:
                    user_filter = [x.strip() for x in args.user_ids.split(',')]
                    logger.info(f"🎯 执行任务化全量标签计算: 用户{user_filter}")
                else:
                    logger.info("🎯 执行任务化全量用户全量标签计算")
                success = scheduler.execute_full_workflow(user_filter)
            except Exception as e:
                logger.error(f"❌ 参数格式错误: {e}")
                sys.exit(1)
        
        elif args.mode == 'list-tasks':
            logger.info("📋 列出所有可用的标签任务:")
            try:
                available_tasks = scheduler.get_available_tasks()
                task_summary = scheduler.get_task_summary()
                
                logger.info("=" * 80)
                logger.info("🏷️  标签任务清单:")
                logger.info("=" * 80)
                
                for tag_id, task_class in available_tasks.items():
                    if tag_id in task_summary:
                        summary = task_summary[tag_id]
                        logger.info(f"""
🆔 标签ID: {tag_id}
📝 任务类: {task_class}
📊 必需字段: {summary['required_fields']}
🗂️  数据源: {summary['data_sources']}
{"─" * 60}""")
                    else:
                        logger.info(f"🆔 标签ID: {tag_id} - 任务类: {task_class}")
                
                logger.info("=" * 80)
                success = True
                
            except Exception as e:
                logger.error(f"❌ 获取任务列表失败: {str(e)}")
                success = False
        
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