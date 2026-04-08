# AGENTS.md

本项目是一个基于 极简的agent系统。

## 开发约定

- 默认先执行 `conda activate num`，再执行 Python 与测试命令；除非用户明确说明已激活 `num`，否则不要直接使用裸 `python` / `pytest`
- 测试必须在项目根目录运行，并优先显式传入测试路径
- 如 pytest 出现 collection 卡住、`rootdir` 误判或 `PermissionError`，优先追加 `--rootdir=.`
- 不要自行安装依赖；如需新增依赖，先询问

常用命令：

```bash
conda activate num
python -m pytest tests/unit_tests -q
python -m pytest tests/integration_tests -q
python -m pytest tests/unit_tests/test_database.py -q --rootdir=.
```

## 代码与文档约定

- Python 代码保持完整类型标注，避免 `Any`
- 生成或修改代码文件时统一使用 `LF` 行尾；读取文本文件时优先显式指定 `UTF-8` 编码
- 注释、docstring、关键说明统一使用中文；公共函数使用中文 Google 风格 docstring
- 设计优先于实现：禁止引入不合理抽象，优先保持代码简洁、清晰、可预测，控制路径复杂度
- 避免过度设计，优先复用现有基础设施
- 避免引入过多的防御性代码

## 测试约定

- 新功能或修复必须补测试
- 单元测试不访问网络，集成测试允许网络
- 测试目录尽量镜像源码结构
- 测试代码与项目代码不一致时，以项目代码为准并修正测试
