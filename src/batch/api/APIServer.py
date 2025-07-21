"""
API服务器启动器 - 重构为驼峰命名风格
统一的API服务器启动和管理
"""

import argparse
import logging
import sys
import signal
from typing import Optional

from .TagTriggerAPI import TagTriggerAPI

logger = logging.getLogger(__name__)


class APIServer:
    """API服务器管理器"""
    
    def __init__(self, environment: str = 'local', host: str = '0.0.0.0', 
                 port: int = 5000, logLevel: str = 'INFO'):
        self.environment = environment
        self.host = host
        self.port = port
        self.logLevel = logLevel
        self.tagTriggerAPI: Optional[TagTriggerAPI] = None
        
        # 配置日志
        self._setupLogging()
        
        # 设置信号处理
        self._setupSignalHandlers()
    
    def _setupLogging(self):
        """配置日志"""
        logFormat = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        logging.basicConfig(
            level=getattr(logging, self.logLevel.upper()),
            format=logFormat,
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler(f'api_server_{self.environment}.log')
            ]
        )
        
        # 设置第三方库日志级别
        logging.getLogger('werkzeug').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        
        logger.info(f"日志配置完成，级别: {self.logLevel}")
    
    def _setupSignalHandlers(self):
        """设置信号处理器"""
        def signalHandler(signum, frame):
            logger.info(f"收到信号 {signum}，开始优雅关闭...")
            self.shutdown()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signalHandler)   # Ctrl+C
        signal.signal(signal.SIGTERM, signalHandler)  # 终止信号
    
    def start(self, debug: bool = False, threaded: bool = True):
        """
        启动API服务器
        
        Args:
            debug: 是否启用调试模式
            threaded: 是否启用多线程模式
        """
        try:
            logger.info("🚀 启动标签系统API服务器...")
            logger.info(f"   🌍 环境: {self.environment}")
            logger.info(f"   🌐 地址: {self.host}:{self.port}")
            logger.info(f"   📊 日志级别: {self.logLevel}")
            logger.info(f"   🐛 调试模式: {debug}")
            
            # 创建并启动TagTriggerAPI
            self.tagTriggerAPI = TagTriggerAPI(env=self.environment)
            
            # 启动服务器
            self.tagTriggerAPI.run(
                host=self.host,
                port=self.port,
                debug=debug
            )
            
        except Exception as e:
            logger.error(f"❌ API服务器启动失败: {str(e)}")
            raise
    
    def shutdown(self):
        """关闭API服务器"""
        logger.info("🛑 正在关闭API服务器...")
        
        try:
            if self.tagTriggerAPI:
                self.tagTriggerAPI.shutdown()
            
            logger.info("✅ API服务器已关闭")
            
        except Exception as e:
            logger.error(f"❌ 关闭API服务器异常: {str(e)}")
    
    def getHealthStatus(self) -> dict:
        """获取服务器健康状态"""
        try:
            if self.tagTriggerAPI:
                # 可以添加更多健康检查逻辑
                return {
                    'status': 'healthy',
                    'environment': self.environment,
                    'host': self.host,
                    'port': self.port
                }
            else:
                return {
                    'status': 'not_started',
                    'environment': self.environment
                }
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)
            }


def parseArguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='标签系统API服务器')
    
    parser.add_argument(
        '--env', 
        choices=['local', 'glue-dev', 'glue-prod'], 
        default='local',
        help='运行环境 (默认: local)'
    )
    
    parser.add_argument(
        '--host', 
        default='0.0.0.0',
        help='服务器主机地址 (默认: 0.0.0.0)'
    )
    
    parser.add_argument(
        '--port', 
        type=int, 
        default=5000,
        help='服务器端口 (默认: 5000)'
    )
    
    parser.add_argument(
        '--log-level', 
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], 
        default='INFO',
        help='日志级别 (默认: INFO)'
    )
    
    parser.add_argument(
        '--debug', 
        action='store_true',
        help='启用调试模式'
    )
    
    return parser.parse_args()


def main():
    """主函数"""
    try:
        # 解析命令行参数
        args = parseArguments()
        
        # 创建API服务器
        server = APIServer(
            environment=args.env,
            host=args.host,
            port=args.port,
            logLevel=args.log_level
        )
        
        # 启动服务器
        server.start(debug=args.debug)
        
    except KeyboardInterrupt:
        logger.info("👋 服务器被用户中断")
    except Exception as e:
        logger.error(f"💥 服务器运行异常: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()