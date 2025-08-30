#!/usr/bin/env python3
"""
海豚调度器图形界面部署包生成器
基于现有S3 Hive能力，为海豚调度器图形界面生成部署包
"""

import os
import zipfile
import tempfile
from pathlib import Path


class DolphinGUIDeployPackager:
    """海豚调度器图形界面部署包生成器"""
    
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.output_dir = self.project_root / "dolphin_gui_deploy"
        self.output_dir.mkdir(exist_ok=True)
    
    
    
    
    def create_main_entry(self) -> str:
        """创建主程序入口 - 支持多环境配置的包装器"""
        main_content = '''#!/usr/bin/env python3
"""
海豚调度器主程序入口（多环境支持版）
直接调用 src/tag_engine/main.py，支持完整的环境配置功能
"""

import sys
import os
import subprocess

def main():
    """主程序入口 - 直接转发到实际的main.py"""
    
    # 获取当前脚本所在目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 构建实际main.py的路径
    actual_main_py = os.path.join(current_dir, "src", "tag_engine", "main.py")
    
    if not os.path.exists(actual_main_py):
        print(f"❌ 找不到主程序文件: {actual_main_py}")
        print("请确认部署包已正确解压")
        return 1
    
    print("🚀 海豚调度器标签系统启动（多环境版）")
    print(f"🔄 转发到: {actual_main_py}")
    print(f"📋 传递参数: {' '.join(sys.argv[1:])}")
    
    try:
        # 构建完整的命令
        cmd = [sys.executable, actual_main_py] + sys.argv[1:]
        
        # 执行实际的main.py，传递所有参数
        result = subprocess.run(cmd, 
                              cwd=current_dir,  # 设置工作目录
                              env=os.environ.copy())  # 传递所有环境变量
        
        return result.returncode
        
    except Exception as e:
        print(f"❌ 执行主程序时出错: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
'''
        return main_content
    
    
    def create_optimized_deploy_guide(self, custom_extract_path: str = None) -> str:
        """创建优化的部署指南 - 支持多环境配置"""
        extract_path = custom_extract_path or "/dolphinscheduler/default/resources/"
        
        guide = f'''# 🐬 海豚调度器图形界面部署指南（精简生产版）

## 📦 部署包内容
- `src/` - 项目源码（完整模块化结构）
- `src/config/config.yaml` - 多环境配置文件（dev/test/pre/prod）
- `requirements.txt` - Python依赖

## 🚀 UI界面部署步骤

### 1. 上传ZIP包到资源中心
1. 登录海豚调度器Web界面
2. 进入 **资源中心** → **文件管理**
3. 上传 `bigdata_tag_system_test.zip`

### 2. 直接在资源中心解压
1. 在资源中心中右键点击上传的ZIP包
2. 选择解压，或者创建Shell任务解压：
```bash
#!/bin/bash
cd {extract_path}
unzip -o bigdata_tag_system_test.zip
echo "✅ 标签系统部署包解压完成到: {extract_path}"
```

### 3. 创建标签计算工作流
1. 创建新工作流："标签计算任务"
2. 添加Spark节点：
   - **主程序**: `{extract_path}src/tag_engine/main.py`
   - **主程序参数**: `--mode health --env test --verbose`
   - **Spark任务名称**: BigDataTagSystem-Dolphin

### 4. Spark任务配置
**基础配置**:
- Driver核心数: 2
- Driver内存: 2g
- Executor数量: 3
- Executor内存: 4g
- Executor核心数: 2

**高级配置**:
- YARN队列: default

## 🎯 支持的执行模式

### 健康检查模式
```bash
--mode health --env test
```
验证Hive和MySQL连接

### 全量标签计算
```bash
--mode task-all --env prod
```
计算所有用户的所有标签

### 指定标签计算
```bash
--mode task-tags --tag-ids 1,2,3 --env prod
```
计算指定标签ID的标签

### 任务列表查看
```bash
--mode list-tasks --env test
```
查看所有可用的标签任务

## 🔧 多环境配置系统

### 支持的环境
- `dev`: 开发环境
- `test`: 测试环境（默认）
- `pre`: 预发环境
- `prod`: 生产环境

### 环境配置文件
配置文件位置: `src/config/config.yaml`
```yaml
# 测试环境示例
test:
  mysql:
    host: cex-mysql-ex-test-cluster.cluster-c5mgk4qm8m2z.ap-southeast-1.rds.amazonaws.com
    port: 3358
    database: biz_user
    user: ex_test_rw
    password: NqaBacRMzCKRRqfEWb
  spark:
    app_name: TagComputeEngine-Test
  hive:
    database: dws_user
```

### 环境变量覆盖
系统支持环境变量覆盖配置：
```bash
export MYSQL_HOST="your-host"
export MYSQL_PORT="3306"
export MYSQL_DATABASE="your-db"
export MYSQL_USER="your-user"
export MYSQL_PASSWORD="your-password"
```

## 💡 最佳实践

### 1. 参数化工作流
在工作流中使用全局参数:
- `environment`: 环境配置 (dev/test/pre/prod)
- `tag_ids`: 标签ID列表
- `mode`: 执行模式

### 2. 环境管理策略
- **测试环境**: 使用 `--env test` 进行功能验证
- **生产环境**: 使用 `--env prod` 执行正式任务
- **配置隔离**: 每个环境独立的MySQL和Spark配置

### 3. 错误处理
- 设置任务失败重试次数: 2
- 设置任务超时时间: 30分钟
- 配置告警通知
- 使用 `--verbose` 获取详细日志

### 4. 监控建议
- 定期执行健康检查任务
- 监控MySQL标签数据增长
- 查看Spark UI资源使用情况
- 跟踪不同环境的任务执行状态

## 🔧 故障排除

### 配置文件问题
```bash
# 检查配置文件是否存在
ls -la {extract_path}src/config/config.yaml

# 验证YAML语法
python3 -c "import yaml; yaml.safe_load(open('{extract_path}src/config/config.yaml'))"
```

### MySQL连接问题
- 检查网络连通性
- 验证用户名密码和数据库权限
- 确认环境配置正确性

### Hive表访问问题
- 验证表是否存在
- 检查分区数据
- 确认权限配置

### 环境切换问题
- 确保使用正确的 `--env` 参数
- 检查对应环境的配置项是否完整
- 验证环境变量覆盖是否生效

## ⚡ 性能优化

### 生产环境优化配置
生产环境已预配置性能优化项：
```yaml
prod:
  spark:
    configs:
      spark.sql.adaptive.advisoryPartitionSizeInBytes: 128MB
      spark.sql.adaptive.coalescePartitions.minPartitionSize: 20MB
```

### 资源调优建议
- 根据数据量调整Executor配置
- 考虑增加并行度
- 优化SQL查询逻辑
- 使用适当的Spark配置参数

## 🚀 升级说明

### 从旧版本升级
1. 备份现有部署
2. 上传新的部署包
3. 更新工作流配置，添加 `--env` 参数
4. 测试健康检查功能
5. 逐步迁移到新的配置系统
'''
        return guide
    
    def create_requirements(self) -> str:
        """创建requirements.txt（海豚调度器环境可选依赖）"""
        requirements = '''# 海豚调度器标签系统依赖
# 注意：PySpark通常已在集群环境中预装，无需安装

# 必需依赖（如果集群环境缺少）
pymysql>=1.0.0       # MySQL连接器

# 可选依赖（通常集群已有）
# pyspark>=3.2.0     # Spark分布式计算引擎（集群预装）
# pandas>=1.3.0      # 数据分析库（集群预装）

# 安装命令（仅在需要时执行）:
# pip3 install pymysql
'''
        return requirements
    
    def create_deploy_guide(self) -> str:
        """创建部署指南"""
        guide = '''# 🐬 海豚调度器图形界面部署指南

## 📦 部署包内容
- `src/` - 项目源码（包含配置管理）
- `requirements.txt` - Python依赖

## 🚀 部署步骤

### 1. 上传到资源中心
1. 登录海豚调度器Web界面
2. 进入 **资源中心** → **文件管理**
3. 上传 `bigdata_tag_system.zip`
4. 直接在资源中心解压到 `/dolphinscheduler/default/resources/`

### 2. 依赖管理
通常无需安装额外依赖，集群环境已预装PySpark。
如需要，可创建Shell任务：
```bash
#!/bin/bash
pip3 install pymysql
echo "✅ 安装MySQL连接器完成"
```

### 3. 健康检查
创建Spark任务测试：
```bash
# 主程序参数  
--mode health
```

## 🎯 任务配置

### Spark任务配置（在海豚图形界面中）
- **主程序**: `/dolphinscheduler/default/resources/main.py`
- **主程序参数**: `--mode health` (根据需要调整)
- **Driver核心数**: 2
- **Driver内存**: 2g
- **Executor数量**: 5
- **Executor内存**: 4g
- **Executor核心数**: 2
- **YARN队列**: default

### 常用主程序参数
```bash
# 健康检查
--mode health

# 全量标签计算
--mode task-all

# 指定标签计算
--mode task-tags --tag-ids 1,2,3

# 列出可用任务
--mode list-tasks
```

## 🔧 Java接口集成（后续）
项目支持通过Java接口触发：
```java
// 通过海豚调度器API触发Spark任务
DolphinSchedulerClient client = new DolphinSchedulerClient();
client.triggerWorkflow("tag_system_compute", Map.of(
    "mode", "task-tags",
    "tag_ids", "1,2,3"
));
```

## 📊 监控和日志
- 通过海豚调度器UI查看任务执行状态
- Spark UI监控资源使用情况
- 任务日志在海豚调度器中查看
'''
        return guide
    
    def create_zip_package(self, custom_extract_path: str = None):
        """创建ZIP部署包，支持自定义解压路径"""
        zip_path = self.output_dir / "bigdata_tag_system.zip"
        
        print("📦 创建海豚调度器部署包...")
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            
            # 添加src目录（包括依赖）
            src_dir = self.project_root / "src"
            if src_dir.exists():
                for file_path in src_dir.rglob("*"):
                    if file_path.is_file():  # 只添加文件，自动创建目录结构
                        arc_name = f"src/{file_path.relative_to(src_dir)}"
                        zip_file.write(file_path, arc_name)
                        if file_path.suffix == ".py":
                            print(f"  ✅ 添加源码: {arc_name}")
                        else:
                            print(f"  📦 添加依赖文件: {arc_name}")
            
            # 保持干净的项目结构，只有一个主程序入口
            print("  ✅ 主程序位置: src/tag_engine/main.py")
            
            # 配置文件已经在src目录遍历时添加过了，不需要重复添加
            print("  ✅ 配置文件已包含: src/config/config.yaml")
            
            # 添加依赖文件
            requirements = self.create_requirements()
            zip_file.writestr("requirements.txt", requirements)
            print("  ✅ 添加依赖: requirements.txt")
        
        print(f"\n🎉 部署包创建完成!")
        print(f"📁 输出目录: {self.output_dir}")
        print(f"📦 ZIP包: {zip_path}")
        print(f"📋 大小: {zip_path.stat().st_size / 1024:.1f} KB")
        
        return zip_path

def main():
    """主函数"""
    packager = DolphinGUIDeployPackager()
    zip_path = packager.create_zip_package()
    
    print(f"\n📋 后续步骤:")
    print(f"1. 上传 {zip_path} 到海豚调度器资源中心")
    print(f"2. 直接在资源中心解压到 /dolphinscheduler/default/resources/")
    print(f"3. 按照 dolphin_gui_deploy/部署说明.md 进行配置")
    print(f"4. 创建Spark任务，主程序路径：/dolphinscheduler/default/resources/src/tag_engine/main.py")

if __name__ == "__main__":
    main()