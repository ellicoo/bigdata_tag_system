#!/bin/bash
echo "🔍 查找资源中心路径..."
find /usr/local/installed/dolphinscheduler -name "tag_system_dolphin.zip" -type f 2>/dev/null
echo "📁 DolphinScheduler安装目录:"
ls -la /usr/local/installed/dolphinscheduler/
echo "🎯 可能的资源目录:"
find /usr/local/installed/dolphinscheduler -type d -name "*resource*" 2>/dev/null