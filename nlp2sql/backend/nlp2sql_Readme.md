# 项目说明

---

## 文件目录

- **/app**  主函数main(终端交互), webapp(前端交互代码)
  - **/static**
    - chat.css  （/chat）聊天界面样式
    - chat.js   单轮对话聊天页面的交互逻辑
    - main.js   调用/api/*的前端逻辑交互，渲染结果
    - style.css 主界面样式
  - **/templates**
    - index.html  步骤式页面，引用main.js、style.css
    - chat.html   聊天界面，引用chat.js、style.css
  - main.py      终端交互式主程序，命令行选择数据库、数据表
    - 选择数据库 → 选择数据表 → 输入自然语言 → 调用 Nlp2SqlAgent 生成 SQL → 执行 → 调用 DataAnalysisAgent → 输出分析/报告/保存临时文件。
  - webapp.py    Flask入口，创建app、注册路由、统一异常处理
    - 典型路由：
      - GET /api/databases 返回数据库列表
      - GET /api/tables?db=xxxx 返回库下的表
      - POST /api/generate_sql{database, table, query} →调用text_to_sql_agent
      - POST /api/execute {database,table,sql} → 执行SQL，返回列、行、以及分析agent的生成报告
      - POST /api/chat 支持多轮对话(获取历史，筛选用户最新的问题)：{history, message,database,table} → 生成SQL + 执行 + 分析消息列表
    - 缓存LLM/Agent实例，避免每次请求初始化
    - 将执行结果中的 Decimal(转为float64)/日期等类型转换为可 JSON 序列化
  - README_flask_ui.md  前端启动说明

- **/agent**    任务执行 Agent 模块
  - __init__.py              包初始化（可集中导出对外 API）
  - base_agent.py            基类与通用工具
  - agent_manager.py         Agent 与依赖的生命周期与单例管理，通过 create 注册对应 agent 并返回实例
  - main_agent.py            编排型 Agent（预留）
  - nlp2sql_agent.py         自然语言转 SQL
    - run(user_nl: str, database: str, table: str, conn: Any) → str（严格一条有效 SELECT；失败返回空）
    - 核心流程：
      1. 根据选择的库、表读取表结构：SHOW FULL COLUMNS → 生成 schema（列名/类型/可空/注释，关于选中表的描述）
      2. 采集每列前 10 个 distinct 值：≤10 视为“有约束”，>10 为“无约束”，注入提示词
      3. 构造提示词：
         - system prompt（约束）
         - user prompt：拼接 schema + 列约束清单（distinct ≤10 的值列表）
      4. 调用 LLM 获取候选 SQL
      5. 后处理
  - data_analysis_agent.py    结果分析/可视化计划与报告（严格 JSON 计划 + 本地计算）

- **/config**   mysql 配置和 LLM 配置
  - /llm
    - .env   大模型的配置参数
    - llm.py 初始化 LLM 实例，通过 create_llm 返回连接的大模型实例(并未确定是哪类型 agent)
  - /sql
    - .env   数据库的配置参数
    - sql.py 初始化数据库连接，通过 create_sql 返回数据库连接实例

- **/tool**     包含了调用的工具函数
  - data_summary   传入 df 格式数据，返回数据总结字符串
  - create_table   传入 df 数据和参数，返回表格路径
  - create_chart   传入 df 数据和参数，返回图表路径

- **/test**     项目检测相关脚本
  - /single_table 单表分析
    - dataset  基于单表分析的九种 jsonl 文件，包含了 question——sql 的问答对
      - test_exec_sql.py  用于检测 jsonl 文件中的 sql 语句是否可以正常执行，执行失败的语句会输出到控制台
    - single_table_test.md 单表分析的常见问题的解析
  - api_test.py      大模型接口测试，用于检测大模型是否正常运行连接
  - connect_mysql.py 检测数据库连接

- **/temp_data**   分析数据的 json 文件和图表文件

---

## 项目流程

### 操作流程

1. 选择数据库  
2. 选择数据表  
3. 输入自然语言问题  
4. 调用 `text_to_sql_agent` 生成 SQL（仅一条有效 SELECT）  
5. 执行 SQL 获取结果集  
6. 调用 `data_analysis_agent` 对结果进行统计与分析  
7. 输出分析报告（文本 + 图表/结构化数据）

> 流程示意：数据库选择 → 表选择 → NL 输入 → NL→SQL → 执行 → 分析 → 报告

### 数据流向

- 前端 (templates + static) → 请求 webapp.py 的 API。
- webapp.py / main.py → 调用 agent 层 (Nlp2SqlAgent & DataAnalysisAgent)。
- agent 层 → 使用 config 中的 LLM/数据库连接工厂 → 访问实际数据库与模型服务。
- 分析结果/中间产物 → 写入 temp_data。

> /api/generate_sql 输入: {database, table, query} 输出: {ok, sql, steps, timing, error?}  
> /api/execute 输入: {database, table, sql} 输出: {ok, result: {columns, rows}, analysis: {...}, steps, error?}  
> /api/chat 输入: {history, message, database?, table?} 输出: {ok, messages: [...], error?}

---

## 运行

### 前端交互

前端说明文件：/home/minshunhua/data/P1/vanna/src/nlp2sql/backend/app/README_flask_ui.md

```bash
cd /home/minshunhua/data/P2
export FLASK_APP=nlp2sql.backend.app.webapp
flask run -h 0.0.0.0 -p 5001 # (port 可换)
```

单轮对话：http://127.0.0.1:5002/chat

### 终端交互

```bash
/bin/python3 /home/minshunhua/data/P2/nlp2sql/backend/app/main.py
```

---

## Supporting

## HighPoints

1. 添加 unique 到 prompt
2. 多轮 agent 判别分析

## Issues

1. sql 执行结果的数据，需要合理保存属性，再进一步分析结果，否则容易影响分析结果（数据的类型转换引起）



