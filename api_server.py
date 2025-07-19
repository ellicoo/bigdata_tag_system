#!/usr/bin/env python3
"""
标签任务触发API服务器启动脚本
"""

import logging
import argparse
import sys
from src.api.tag_trigger_api import TagTriggerAPI

def setup_logging(level=logging.INFO):
    """设置日志配置"""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('api_server.log')
        ]
    )
    
    # 抑制第三方库的冗余日志
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    logging.getLogger("py4j").setLevel(logging.ERROR)

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='启动标签任务触发API服务器')
    parser.add_argument('--env', default='local', choices=['local', 'glue-dev', 'glue-prod'],
                       help='运行环境 (默认: local)')
    parser.add_argument('--host', default='0.0.0.0', help='监听地址 (默认: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=5000, help='监听端口 (默认: 5000)')
    parser.add_argument('--debug', action='store_true', help='开启调试模式')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='日志级别 (默认: INFO)')
    
    args = parser.parse_args()
    
    # 设置日志
    log_level = getattr(logging, args.log_level.upper())
    setup_logging(log_level)
    
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"🚀 启动标签任务触发API服务器")
        logger.info(f"🌍 环境: {args.env}")
        logger.info(f"🔗 监听地址: {args.host}:{args.port}")
        logger.info(f"🐛 调试模式: {args.debug}")
        
        # 创建并运行API服务器
        api = TagTriggerAPI(env=args.env)
        api.run(host=args.host, port=args.port, debug=args.debug)
        
    except KeyboardInterrupt:
        logger.info("🛑 收到停止信号，正在关闭服务器...")
    except Exception as e:
        logger.error(f"❌ 服务器启动失败: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()