# Flask 前端展示 main() 运行流程

本界面将终端交互版的 `main()` 流程（选库/选表、自然语言转 SQL、执行查询、AI 分析）搬到网页端，并以步骤进度的形式展示。

## 运行

1) 准备环境变量（示例为 MySQL）：

```bash
export DB_HOST=localhost
export DB_USER=root
export DB_PASSWORD=123456
export DB_PORT=3306
# 可选：默认数据库
# export DB_NAME=demo

# LLM 配置（依据你的实际部署）
export MODEL_NAME="qwen3-32B"
export MODEL_SERVER="http://<your-llm-server>:9033"
export API_KEY="key"
```

2) 安装依赖（如果尚未安装）：

```bash
pip install -e .
```

3) 启动服务：

```bash
# 方法一（推荐）
export FLASK_APP=nlp2sql.backend.app.webapp
flask run -h 0.0.0.0 -p 5000

# 方法二
PYTHONPATH=$(pwd)/src python -m nlp2sql.backend.app.webapp
```

访问 http://localhost:5000

## 功能说明
- “加载数据库” 按钮：调用 `/api/databases` 列出可用数据库
- 选择数据库后点 “加载表”：调用 `/api/tables?db=xxx` 列出该库下的表
- 输入自然语言点 “生成 SQL”：调用 `/api/generate_sql` 使用 LLM 生成 SQL
- 可在文本框中编辑 SQL，点 “执行 SQL”：调用 `/api/execute` 执行并展示结果 + AI 分析

## 进阶（可选）
- 实时流式进度：可使用 WebSocket（本项目已包含 `flask-sock` 依赖）为长耗时步骤推送日志。当前实现采用同步步骤列表返回，已能满足大多数场景；如需流式可后续扩展。
