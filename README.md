# 跨节点算力预留与履约

本项目提供一组 Python 基础领域契约，用于表达跨节点算力预留与履约中的稳定标识、不可变版本、内容摘要和冲突检测。仓库当前只包含可运行的领域起点，便于后续在同一服务中扩展持久化、状态流转、权限、API 与审计能力，不依赖浏览器、外部数据库或其他运行服务。

## 运行环境

- Python 3.11 或更高版本
- Linux、macOS 或 Windows

## 运行测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 编译检查

```bash
python3 -m compileall -q src tests run_cli.py
```

## 命令行冒烟

```bash
python3 run_cli.py
```

命令会输出一个示例对象及其稳定摘要，可用于确认基础契约能够正常加载和执行。
