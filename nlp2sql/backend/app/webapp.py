"""
Flask 前端：以可视化方式展示 main() 的运行流程

功能
- 列出数据库与表
- 输入自然语言并生成 SQL
- 接受/编辑 SQL 并执行
- 展示查询结果与 AI 分析
- 前端展示步骤进度

依赖：flask, flask-sock 已在 pyproject.toml 中声明
运行：
  export FLASK_APP=nlp2sql.backend.app.webapp
  flask run -h 0.0.0.0 -p 5000
或者：
  python -m nlp2sql.backend.app.webapp
"""
from __future__ import annotations
from pathlib import Path
import sys
import json
from decimal import Decimal
import time
from typing import Any, Dict, List
import re

import pandas as pd
from flask import Flask, render_template, jsonify, request

# 让 backend 包可以被正确导入
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from config.sql.sql import create_db  # noqa: E402
from config.llm.llm import create_llm  # noqa: E402


def rows_to_list(rows) -> list[str]:
    out = []
    if not rows:
        return out
    for r in rows:
        try:
            if isinstance(r, dict):
                vals = list(r.values())
                out.append(str(vals[0]) if vals else str(r))
            elif isinstance(r, (list, tuple)):
                out.append(str(r[0]) if r else str(r))
            else:
                out.append(str(r))
        except Exception:
            out.append(str(r))
    return out


def _coerce_decimal_to_float(df: pd.DataFrame) -> pd.DataFrame:
    """将 Decimal/decimal 列转换为 float64，避免被当作 object。"""
    try:
        # 1) 处理 pyarrow decimal 扩展类型
        for col in df.columns:
            dt_str = str(df[col].dtype).lower()
            if "decimal" in dt_str:
                try:
                    df[col] = df[col].astype("float64[pyarrow]")
                except Exception:
                    try:
                        df[col] = df[col].astype("float64")
                    except Exception:
                        pass

        # 2) 处理 object 列中含有 Decimal 的情况
        for col in df.columns:
            s = df[col]
            if s.dtype == object:
                sample = s.dropna()
                has_decimal = any(isinstance(v, Decimal) for v in sample.head(1000))
                if has_decimal:
                    s2 = s.apply(lambda v: float(v) if isinstance(v, Decimal) else (None if v is None else v))
                    df[col] = pd.to_numeric(s2, errors='coerce')
    except Exception:
        return df
    return df


def results_to_dataframe(results, columns=None) -> pd.DataFrame:
    try:
        import pyarrow  # noqa: F401
        df = pd.DataFrame(results, columns=columns, dtype_backend="pyarrow")
        return _coerce_decimal_to_float(df)
    except Exception:
        df = pd.DataFrame(results, columns=columns)
        try:
            df = df.convert_dtypes()
        except Exception:
            pass
        return _coerce_decimal_to_float(df)


def df_to_records(df: pd.DataFrame) -> Dict[str, Any]:
    """将 DataFrame 转为前端友好格式。
    返回：{ columns: [..], rows: [[..], ..] }
    """
    cols = [str(c) for c in df.columns]
    rows = df.astype(object).where(pd.notnull(df), None).values.tolist()
    return {"columns": cols, "rows": rows}


def normalize_sql(generated: Any) -> str:
    """尽量从返回对象中提取 SQL 文本，去除 ```sql ... ``` 包裹等。
    支持：
    - 直接字符串
    - dict 包含 'sql' 字段
    - 代码块三引号包裹
    """
    if generated is None:
        return ""
    if isinstance(generated, dict):
        # 常见形态：{"sql": "select ..."}
        val = generated.get("sql")
        if isinstance(val, str):
            generated = val
        else:
            # 回退到字符串化
            generated = json.dumps(generated, ensure_ascii=False)
    elif not isinstance(generated, str):
        generated = str(generated)

    s = generated.strip()
    # 去除 ```sql ... ``` 或 ``` 包裹
    fence = re.compile(r"^```(?:sql)?\s*\n([\s\S]*?)\n```\s*$", re.IGNORECASE)
    m = fence.match(s)
    if m:
        s = m.group(1).strip()
    # 去掉多余分号之外的内容（保留末尾分号可选）
    return s


app = Flask(__name__, template_folder="templates", static_folder="static")

# --- 全局缓存，避免每次请求都创建 LLM/Agent 导致等待时间长 ---
LLM_SINGLETON = None
TEXT2SQL_SINGLETON = None
ANALYSIS_SINGLETON = None
SQL_JUDGE_SINGLETON = None


def get_llm():
    global LLM_SINGLETON
    if LLM_SINGLETON is None:
        LLM_SINGLETON = create_llm()
    return LLM_SINGLETON


def get_text2sql_agent():
    global TEXT2SQL_SINGLETON
    if TEXT2SQL_SINGLETON is None:
        TEXT2SQL_SINGLETON = get_llm().text_to_sql_agent()
    return TEXT2SQL_SINGLETON


def get_analysis_agent():
    global ANALYSIS_SINGLETON
    if ANALYSIS_SINGLETON is None:
        ANALYSIS_SINGLETON = get_llm().data_analysis_agent()
    return ANALYSIS_SINGLETON


def get_sql_judge_agent():
    global SQL_JUDGE_SINGLETON
    if SQL_JUDGE_SINGLETON is None:
        SQL_JUDGE_SINGLETON = get_llm().sql_judge_agent()
    return SQL_JUDGE_SINGLETON


def generate_sql_with_validation(user_query: str, db_name: str, table_name: str, db) -> dict:
    """
    闭环：生成SQL -> 判别 -> 根据建议重生成（最多n轮,这里n取3）。
    返回：{ ok, sql, iterations: [...judge...], last_judge }
    """
    nlp_agent = get_text2sql_agent()
    judge_agent = get_sql_judge_agent()
    iterations = []
    fix = None
    sql = nlp_agent.run(user_nl=user_query, database=db_name, table=table_name, conn=db, fix_suggestion=fix)
    last_judge = None
    for _ in range(3):
        jr = judge_agent.run(user_query, sql)
        # 记录每轮判别及对应SQL
        it = dict(jr)
        it["sql"] = sql
        iterations.append(it)
        last_judge = jr
        if jr.get("valid"):
            return {"ok": True, "sql": sql, "iterations": iterations, "last_judge": last_judge}
        fix = jr.get("fix_suggestion") or ""
        sql = nlp_agent.run(user_nl=user_query, database=db_name, table=table_name, conn=db, fix_suggestion=fix)
    return {"ok": False, "sql": sql, "iterations": iterations, "last_judge": last_judge}



# 聊天界面
@app.route("/chat")
def chat_page():
    return render_template("chat.html")

# 旧首页保留
@app.route("/")
def index():
    return render_template("index.html")
from flask import request

# 聊天多轮对话 API
@app.post("/api/chat")
def api_chat():
    data = request.get_json(force=True)
    history = data.get("history", [])
    db_name = data.get("db")
    table_name = data.get("table")
    # 取最后一条用户消息
    user_msg = None
    for m in reversed(history):
        if m.get("role") == "user":
            user_msg = m.get("content")
            break
    if not user_msg:
        return {"messages": [{"type": "text", "content": "未检测到用户输入。"}]}

    # 1. 生成 SQL
    db = create_db()
    if not db.connect_to_database():
        return {"messages": [{"type": "text", "content": "数据库连接失败。"}]}
    try:
        if db_name and not db.select_database(db_name):
            return {"messages": [{"type": "text", "content": f"无法切换到数据库 {db_name}"}]}
        # 先进行带判别与修复闭环的 SQL 生成
        loop_res = generate_sql_with_validation(user_msg, db_name, table_name, db)
        sql = normalize_sql(loop_res.get("sql") or "")
        msgs = []
        # 输出判别过程
        for it in (loop_res.get("iterations") or []):
            valid = bool(it.get("valid"))
            reason = it.get("reason", "")
            fix_suggestion = it.get("fix_suggestion", "")
            it_sql = it.get("sql", "")
            status = "通过" if valid else "失败"
            content = f"[判别{status}]\nSQL: {it_sql}\n原因: {reason}\n修复建议: {fix_suggestion}"
            msgs.append({"type": "judge", "valid": valid, "content": content})
        if sql:
            msgs.append({"type": "sql", "content": sql})
            # 2. 执行 SQL
            results = db.execute_query(sql)
            if results and isinstance(results, list) and results and isinstance(results[0], dict):
                df = results_to_dataframe(results)
                preview_df = df.head(100)
                msgs.append({"type": "table", "data": df_to_records(preview_df)})
                # 3. 分析
                analysis_agent = get_analysis_agent()
                analysis_table = analysis_agent.run(df, user_msg or "", mode='table')
                analysis_report = analysis_agent.run(df, user_msg or "", mode='report')
                msgs.append({"type": "analysis", "data": str(analysis_table)})
                msgs.append({"type": "analysis", "data": str(analysis_report)})
            else:
                msgs.append({"type": "text", "content": "无结果或查询失败。"})
        else:
            msgs.append({"type": "text", "content": "未能生成有效 SQL。"})
        return {"messages": msgs}
    except Exception as e:
        return {"messages": [{"type": "text", "content": f"出错: {e}"}]}
    finally:
        db.close()


@app.get("/api/databases")
def api_databases():
    steps = ["连接数据库", "列出数据库"]
    db = create_db()
    if not db.connect_to_database():
        return jsonify({"ok": False, "error": "数据库连接失败", "steps": steps}), 500
    try:
        res = db.execute_query("SHOW DATABASES;")
        names = rows_to_list(res)
        return jsonify({"ok": True, "databases": names, "steps": steps})
    finally:
        db.close()


@app.get("/api/tables")
def api_tables():
    chosen_db = request.args.get("db")
    if not chosen_db:
        return jsonify({"ok": False, "error": "缺少参数 db"}), 400
    steps = [f"切换数据库: {chosen_db}", "列出表"]
    db = create_db()
    if not db.connect_to_database():
        return jsonify({"ok": False, "error": "数据库连接失败", "steps": steps}), 500
    try:
        if not db.select_database(chosen_db):
            return jsonify({"ok": False, "error": "无法切换到所选数据库", "steps": steps}), 400
        res = db.execute_query(f"SHOW TABLES FROM `{chosen_db}`;")
        names = rows_to_list(res)
        return jsonify({"ok": True, "tables": names, "steps": steps})
    finally:
        db.close()


@app.post("/api/generate_sql")
def api_generate_sql():
    data = request.get_json(force=True)
    database = data.get("database")
    table = data.get("table")
    user_nl = data.get("query", "").strip()
    if not database or not table or not user_nl:
        return jsonify({"ok": False, "error": "参数不完整(database/table/query)"}), 400

    t0 = time.time()
    steps = [f"选择数据库: {database}", f"选择表: {table}"]
    db = create_db()
    if not db.connect_to_database():
        return jsonify({"ok": False, "error": "数据库连接失败", "steps": steps}), 500
    try:
        if not db.select_database(database):
            return jsonify({"ok": False, "error": "无法切换到所选数据库", "steps": steps}), 400

        t1 = time.time()
        loop_res = generate_sql_with_validation(user_nl, database, table, db)
        t3 = time.time()
        steps += [f"生成+判别闭环: {(t3 - t1):.2f}s"]
        if not loop_res.get("ok"):
            return jsonify({
                "ok": False,
                "error": "未能在多轮修复内生成有效 SQL",
                "judge": loop_res,
                "steps": steps,
                "timing": {"total": round(t3 - t0, 2)}
            }), 200
        return jsonify({
            "ok": True,
            "sql": loop_res.get("sql"),
            "judge": loop_res,
            "steps": steps,
            "timing": {"total": round(t3 - t0, 2)}
        })
    except Exception as e:
        return jsonify({"ok": False, "error": f"生成 SQL 失败: {e}", "steps": steps}), 500
    finally:
        db.close()


@app.post("/api/generate_sql_validated")
def api_generate_sql_validated():
    data = request.get_json(force=True)
    database = data.get("database")
    table = data.get("table")
    user_nl = data.get("query", "").strip()
    if not database or not table or not user_nl:
        return jsonify({"ok": False, "error": "参数不完整(database/table/query)"}), 400

    t0 = time.time()
    steps = [f"选择数据库: {database}", f"选择表: {table}"]
    db = create_db()
    if not db.connect_to_database():
        return jsonify({"ok": False, "error": "数据库连接失败", "steps": steps}), 500
    try:
        if not db.select_database(database):
            return jsonify({"ok": False, "error": "无法切换到所选数据库", "steps": steps}), 400
        t1 = time.time()
        loop_res = generate_sql_with_validation(user_nl, database, table, db)
        t3 = time.time()
        steps += [f"生成+判别闭环: {(t3 - t1):.2f}s"]
        return jsonify({
            "ok": bool(loop_res.get("ok")),
            "sql": loop_res.get("sql"),
            "judge": loop_res,
            "steps": steps,
            "timing": {"total": round(t3 - t0, 2)}
        })
    except Exception as e:
        return jsonify({"ok": False, "error": f"生成 SQL 失败: {e}", "steps": steps}), 500
    finally:
        db.close()


@app.post("/api/execute")
def api_execute():
    data = request.get_json(force=True)
    database = data.get("database")
    sql_to_exec = data.get("sql", "").strip()
    user_nl = data.get("query", "").strip()
    table = data.get("table", "")
    if not database or not sql_to_exec:
        return jsonify({"ok": False, "error": "参数不完整(database/sql)"}), 400

    t0 = time.time()
    steps = [f"切换数据库: {database}", "执行 SQL", "转为 DataFrame", "数据分析"]
    db = create_db()
    if not db.connect_to_database():
        return jsonify({"ok": False, "error": "数据库连接失败", "steps": steps}), 500

    try:
        if not db.select_database(database):
            return jsonify({"ok": False, "error": "无法切换到所选数据库", "steps": steps}), 400

        results = db.execute_query(sql_to_exec)
        if not results:
            return jsonify({"ok": False, "error": "无结果或查询失败", "steps": steps}), 200

        # 结果 -> DataFrame
        if isinstance(results, list) and results and isinstance(results[0], dict):
            df = results_to_dataframe(results)
        elif isinstance(results, list) and results and isinstance(results[0], (list, tuple)):
            # 若返回元组列表（理论上不会，因为使用了 DictCursor）
            df = results_to_dataframe(results)
        else:
            return jsonify({"ok": False, "error": "结果格式不支持自动分析", "steps": steps}), 200

        # 调用数据分析 Agent
        t1 = time.time()
        analysis_agent = get_analysis_agent()
        t2 = time.time()
        analysis_table = analysis_agent.run(df, user_nl or "", mode='table')
        analysis_report = analysis_agent.run(df, user_nl or "", mode='report')
        t3 = time.time()
        steps += [f"分析 Agent 就绪: {(t2 - t1):.2f}s", f"分析用时: {(t3 - t2):.2f}s"]

        # 限制行数返回，避免过大
        preview_df = df.head(100)
        payload = {
            "ok": True,
            "steps": steps,
            "result": df_to_records(preview_df),
            "analysis": {
                "table": analysis_table,
                "report": analysis_report,
            },
        }
        payload["timing"] = {"total": round(time.time() - t0, 2)}
        return jsonify(payload)
    except Exception as e:
        return jsonify({"ok": False, "error": f"执行失败: {e}", "steps": steps}), 500
    finally:
        db.close()


def _run_dev():
    app.run(host="0.0.0.0", port=5002, debug=True)


if __name__ == "__main__":
    _run_dev()
