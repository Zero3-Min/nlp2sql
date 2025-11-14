from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional, Tuple
import json
import logging
import re

try:
    from sqlglot import exp, parse_one
    from sqlglot.errors import ParseError
except Exception:  # pragma: no cover - runtime dependency guard
    parse_one = None
    exp = None  # type: ignore
    ParseError = Exception  # type: ignore

from .base_agent import BaseAgent


class SqlJudgeAgent(BaseAgent):
    """多层 SQL 判别 Agent。

    职责：
      1. 通过 LLM 做语义一致性判断与 SQL→NL 解释
      2. EXPLAIN/LIMIT 0 级别的可执行性预检查
      3. 汇总为统一 JSON，并将结果整合sql语句，一起检查，必要时给出修复建议
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.llm_assistant = kwargs.get("llm_assistant")
        self._logger = logging.getLogger(self.__class__.__name__)
        self.similarity_threshold: float = float(kwargs.get("similarity_threshold", 0.82))


    # ----------------------------------
    # 语义 LLM 校验（含 SQL→NL）
    # ----------------------------------
    def _semantic_alignment(self, user_query: str, sql_generated: str, schema: Optional[Dict[str, Any]],) -> Dict[str, Any]:
        if not self.llm_assistant:
            return {
                "valid": False,
                "reason": "判别模型未配置",
                "fix_suggestion": "",
                "sql_nl_explanation": "",
                "errors": ["缺少 LLM 判别模块"],
            }

        schema_lines: List[str] = []
        if schema:
            schema_lines.append(
                f"数据库: {schema.get('database')}, 表: {schema.get('table')}"
            )
            for col in schema.get("columns", []):
                schema_lines.append(
                    f"- {col.get('name')} ({col.get('type', '')})"
                )
        schema_text = "\n".join(schema_lines) if schema_lines else "未知"

        system_prompt = (
        '''
            你是一个专业的 MySQL 数据库专家和高级业务分析师，职责是：
            1) 判断模型生成的 SQL 是否满足用户的自然语言需求（语义一致性判定）
            2) 判断 SQL 是否在 MySQL 语法和功能范围内（能力一致性判定）
            3) 将 SQL 转换成自然语言解释（SQL→NL）
            4) 如果 SQL 不正确，给出具体的修复建议（Fix Suggestion）
            5) 最终输出结构化 JSON（严格遵守字段要求）

            你必须严格执行下述原则：

            ====================
            【A. 语义一致性判定规则】
            ====================
            你需要判断 SQL 是否真正满足用户的问题，而不是仅仅语法正确。
            特别关注以下错误场景：

            1. 分组语义错误
            “各医院 / 按医院 / 每个医院” → 必须 GROUP BY hospital_id
            “全部医院的平均值” → 不应该分组

            2. 聚合方向错误
            用户要求“求总数”，却用了 AVG()
            用户要求“求平均值”，却用了 SUM()
            用户要求“前 N 名”，却未排序或排序方向错误

            3. 过滤条件遗漏
            用户问题中包含日期区间、医院、状态、级别、科室等过滤条件
            SQL 中却没有 WHERE 对应过滤

            4. 范围 / 时间语义误解
            “最近7天” → 需要 NOW() - INTERVAL 7 DAY
            “今年” → YEAR(date_col) = YEAR(CURDATE())

            5. 排序语义错误
            “最高的”“最大的”“前 N 名” → 必须 DESC
            “最低的”“最小的” → 必须 ASC
            用户未指定 → 默认 ASC（升序）

            6. 字段误解
            “医院数量” ≠ “床位数量”
            “医生数量” ≠ “医生表中的科室数量”

            7. 多字段同时比较
            用户要求“每个医院、每个科室”，SQL 却只按医院分组

            你必须基于语义判断，而不是仅看 SQL 表面的结构。

            ====================
            【B. MySQL 能力一致性检查】
            ====================
            请检查 SQL 是否能在 MySQL 中正常执行。

            包括但不限于：

            1. 禁止在 MySQL 中使用不支持的关键字  
            如 TOP、QUALIFY、DISTINCT ON 等

            2. 窗口函数必须符合 MySQL 8.0 规范  
            - COUNT(*) OVER(PARTITION BY) 可以  
            - MAX(COUNT(*)) OVER(...)（嵌套聚合）不合法  

            3. HAVING 中使用别名的行为需符合 MySQL 规则

            4. 子查询必须能返回对应维度  
            不允许多列不匹配的 IN 子查询

            如果 SQL 存在“语义正确但 MySQL 无法执行”的情况，你必须判定为 **semantic_valid = false** 并给出修复建议。

            ====================
            【C. SQL → 自然语言解释规则】
            ====================
            你必须输出一段自然语言解释，让用户可以理解 SQL 做了什么。
            解释需包含：
            - 查询了哪张表
            - 过滤条件是什么
            - 是否分组
            - 是否聚合
            - 排序规则
            - 结果代表什么含义

            ====================
            【D. 输出格式（JSON）】
            ====================
            最终输出必须是 JSON，对象结构如下：

            {
            "semantic_valid": true/false,         # SQL 是否满足用户意图（语义一致性）
            "semantic_reason": "原因说明",        # 若 false，说明哪里不一致
            "sql_nl_explanation": "SQL 的自然语言解释",
            "fix_suggestion": "如需修改，给出具体修复建议；否则为空字符串",
            "confidence": 0.00 ~ 1.00            # 判定置信度
            }

            要求：
            - 必须输出 JSON
            - 不得在 JSON 外输出任何多余内容
            - 所有字符串必须是单行文本，不得包含换行符
            - 字段必须全部包含，即使为空也必须返回
        ''')
        
        user_prompt = (
            f"用户问题: {user_query}\n"
            f"SQL: {sql_generated}\n"
            f"表结构:{schema_text}\n"
            "请严格输出 JSON，不要附加说明。"
        )

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        raw_text = self._get_last_text(self.llm_assistant, messages)
        data = self._parse_json(raw_text)
        semantic_valid = bool(data.get("semantic_valid"))
        semantic_reason = str(data.get("semantic_reason", "")).strip()
        sql_explanation = str(data.get("sql_nl_explanation", "")).strip()
        fix_suggestion = str(data.get("fix_suggestion", "")).strip()
        confidence = data.get("confidence")
        try:
            confidence = float(confidence) if confidence is not None else None
        except Exception:
            confidence = None

        errors: List[str] = []
        if not semantic_valid:
            if semantic_reason:
                errors.append(semantic_reason)
            else:
                errors.append("语义判定失败：缺少具体原因")

        return {
            "valid": semantic_valid,
            "reason": semantic_reason,
            "fix_suggestion": fix_suggestion,
            "sql_nl_explanation": sql_explanation,
            "confidence": confidence,
            "errors": errors,
        }

    # ----------------------------------
    # 执行前检查
    # ----------------------------------
    def _execution_check(self, sql: str, db: Any) -> Dict[str, Any]:
        info: Dict[str, Any] = {
            "valid": True,
            "method": "EXPLAIN",
            "errors": [],
        }
        clean_sql = (sql or "").strip().rstrip(";")
        if not clean_sql:
            info["valid"] = False
            info["errors"].append("SQL 为空，无法执行预检查")
            return info

        if not db or not hasattr(db, "execute_query"):
            info["valid"] = False
            info["errors"].append("缺少数据库连接，无法执行 EXPLAIN")
            return info

        explain_sql = f"EXPLAIN {clean_sql}"
        result = None
        try:
            result = db.execute_query(explain_sql)
        except Exception as exc:  # pragma: no cover - 执行兜底
            self._logger.warning("EXPLAIN 执行异常: %s", exc)
            result = None

        if result is None:
            # 尝试 LIMIT 0
            info["method"] = "LIMIT 0"
            limit_sql = clean_sql
            if " limit " not in clean_sql.lower():
                limit_sql = f"{clean_sql} LIMIT 0"
            try:
                result = db.execute_query(limit_sql)
            except Exception as exc:  # pragma: no cover
                self._logger.warning("LIMIT 0 执行异常: %s", exc)
                result = None
        if result is None:
            info["valid"] = False
            info["errors"].append("EXPLAIN/LIMIT 0 执行失败，SQL 可能无法运行")
        return info

    # ----------------------------------
    # 工具方法
    # ----------------------------------
    def _parse_json(self, text: str) -> Dict[str, Any]:
        s = (text or "").strip()
        fence = re.compile(r"^```(?:json)?\s*\n([\s\S]*?)\n```\s*$", re.IGNORECASE)
        m = fence.match(s)
        if m:
            s = m.group(1).strip()
        try:
            obj = json.loads(s)
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass
        try:
            start = s.find("{")
            end = s.rfind("}")
            if start != -1 and end != -1 and end > start:
                obj = json.loads(s[start : end + 1])
                if isinstance(obj, dict):
                    return obj
        except Exception:
            pass
        return {}
    

    def _aggregate_errors(self, *items: Tuple[Dict[str, Any], str]) -> List[str]:
        errors: List[str] = []
        for info, default_msg in items:
            if not info:
                continue
            if info.get("valid"):
                continue
            errs = info.get("errors")
            if isinstance(errs, list) and errs:
                errors.extend(str(e) for e in errs if e)
            elif default_msg:
                errors.append(default_msg)
        return [e for e in errors if e]


    def fetch_table_schema(self, db, database: str, table: str) -> Dict[str, Any]:
        schema: Dict[str, Any] = {"database": database, "table": table, "columns": []}
        sql = f"SHOW FULL COLUMNS FROM `{database}`.`{table}`;"
        try:
            rows = db.execute_query(sql)
        except Exception:
            rows = None
        if not rows:
            return schema
        columns: List[Dict[str, Any]] = []
        for row in rows:
            name = row.get("Field") or row.get("COLUMN_NAME") or row.get("field")
            if not name:
                continue
            columns.append(
                {
                    "name": name,
                    "type": row.get("Type")
                    or row.get("COLUMN_TYPE")
                    or row.get("type")
                    or "",
                    "nullable": (row.get("Null") or row.get("IS_NULLABLE") or "").upper()
                    in ("YES", "TRUE", "Y"),
                    "comment": row.get("Comment") or row.get("COLUMN_COMMENT") or "",
                }
            )
        schema["columns"] = columns
        return schema

    # 简化版文本流读取（不做分号裁剪）
    def _get_last_text(self, assistant, messages, stream: bool = True) -> str:
        text = ""
        DELIM = "\n<CHUNK_END>\n"
        try:
            for chunk in assistant.run(messages=messages, stream=stream):
                if isinstance(chunk, list):
                    for item in chunk:
                        if isinstance(item, dict):
                            part = item.get("content", "") or item.get("reasoning_content", "")
                            if part:
                                text += part + DELIM
                        elif isinstance(item, str):
                            text += item + DELIM
                elif isinstance(chunk, dict):
                    part = chunk.get("content", "") or chunk.get("reasoning_content", "")
                    if part:
                        text += part + DELIM
                elif isinstance(chunk, str):
                    text += chunk + DELIM
                else:
                    try:
                        for item in chunk:
                            if isinstance(item, dict):
                                part = item.get("content", "") or item.get("reasoning_content", "")
                                if part:
                                    text += part + DELIM
                            elif isinstance(item, str):
                                text += item + DELIM
                    except Exception:
                        pass
            if DELIM in text:
                parts = [p.strip() for p in text.split(DELIM) if p.strip()]
                result = parts[-1] if parts else ""
            else:
                result = text.strip()
            return result
        except Exception:
            return ""

    def run(
        self,
        user_query: str,
        sql_generated: str,
        db_name: Optional[str] = None,
        table_name: Optional[str] = None,
        schema: Optional[Dict[str, Any]] = None,
        db: Any = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        # 兼容旧调用：某些地方可能通过关键字参数传入 schema/db 
        if db is None and "db" in kwargs:
            db = kwargs.get("db")
        if schema is None and "schema" in kwargs:
            schema = self.fetch_table_schema(db, db_name, table_name)
        try:
            semantic_info = self._semantic_alignment(user_query, sql_generated, schema)
            explanation = semantic_info.get("sql_nl_explanation", "")
            sql2nl_info = {
                "valid": bool(explanation),
                "explanation": explanation,
                "errors": [] if explanation else ["未生成 SQL 自然语言解释"],
            }
            execution_info = self._execution_check(sql_generated, db)

            combined_errors = self._aggregate_errors(
                (semantic_info, "语义判定失败"),
                (sql2nl_info, "SQL→自然语言解释缺失"),
                (execution_info, "SQL 无法执行"),
            )

            valid = (
                semantic_info.get("valid")
                and sql2nl_info.get("valid")
                and execution_info.get("valid")
            )

            reason = combined_errors[0] if combined_errors else "SQL 校验通过"
            fix_candidates: List[str] = []
            for item in (semantic_info, execution_info):
                fix = item.get("fix_suggestion") if isinstance(item, dict) else None
                if fix:
                    fix_candidates.append(str(fix))
            fix_suggestion = next((f for f in fix_candidates if f), "")
            if not fix_suggestion and combined_errors:
                fix_suggestion = combined_errors[0]

            result = {
                "valid": bool(valid),
                "errors": combined_errors,
                "reason": reason,
                "fix_suggestion": fix_suggestion,
                "sql_nl_explanation": explanation,
                "need_regenerate": not bool(valid),
                "details": {
                    "semantic": semantic_info,
                    "sql2nl": sql2nl_info,
                    "execution": execution_info,
                },
            }
            return result
        except Exception as exc:  # pragma: no cover - 整体容错
            self._logger.exception("SqlJudgeAgent 失败: %s", exc)
            return {
                "valid": False,
                "errors": [f"判别异常: {exc}"],
                "reason": f"判别异常: {exc}",
                "fix_suggestion": "请检查 SQL 语法、字段及分组条件后重试",
                "sql_nl_explanation": "",
                "need_regenerate": True,
                "details": {},
            }

    