from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional, Tuple
import json
import logging
import math
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
      1. sqlglot 语法解析 + 字段合法性检查
      2. 通过 LLM 做语义一致性判断与 SQL→NL 解释
      3. Query 与 SQL 自然语言解释的相似度校验
      4. EXPLAIN/LIMIT 0 级别的可执行性预检查
      5. 汇总为统一 JSON，必要时给出修复建议
    """

    AGGREGATE_FUNCTIONS = {
        "avg",
        "count",
        "count_distinct",
        "sum",
        "max",
        "min",
        "stddev",
        "stddev_pop",
        "stddev_samp",
        "variance",
        "var_pop",
        "var_samp",
    }

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.llm_assistant = kwargs.get("llm_assistant")
        self._logger = logging.getLogger(self.__class__.__name__)
        self.similarity_threshold: float = float(kwargs.get("similarity_threshold", 0.82))

    # ----------------------------------
    # 语法与结构校验
    # ----------------------------------
    def _check_syntax(self, sql: str, schema: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        info: Dict[str, Any] = {
            "valid": True,
            "errors": [],
            "columns_used": [],
            "unknown_columns": [],
            "aggregated_columns": [],
            "non_aggregated_columns": [],
            "group_by_columns": [],
            "tables": [],
        }
        clean_sql = (sql or "").strip().rstrip(";")
        if not clean_sql:
            info["valid"] = False
            info["errors"].append("SQL 为空")
            return info

        if parse_one is None or exp is None:
            info["valid"] = False
            info["errors"].append("sqlglot 未安装，无法执行语法校验")
            return info

        try:
            parsed = parse_one(clean_sql, read="mysql")
            info["ast"] = parsed.to_s()
        except ParseError as exc:
            info["valid"] = False
            info["errors"].append(f"SQL 解析失败: {exc}")
            return info
        except Exception as exc:  # pragma: no cover - 容错兜底
            info["valid"] = False
            info["errors"].append(f"SQL 解析异常: {exc}")
            return info

        # 表名提取
        tables = {
            self._normalize_identifier(tbl.name)  # type: ignore[arg-type]
            for tbl in parsed.find_all(exp.Table)
            if getattr(tbl, "name", None)
        }
        info["tables"] = sorted(tables)

        schema_columns = {
            self._normalize_identifier(col.get("name"))
            for col in (schema or {}).get("columns", [])
            if col.get("name")
        }

        # 列检查
        used_columns: set[str] = set()
        for col in parsed.find_all(exp.Column):
            col_name = self._normalize_identifier(getattr(col, "name", None))
            if not col_name or col_name == "*":
                continue
            used_columns.add(col_name)
        info["columns_used"] = sorted(used_columns)

        if schema_columns:
            unknown_cols = sorted(c for c in used_columns if c not in schema_columns)
            if unknown_cols:
                info["valid"] = False
                info["unknown_columns"] = unknown_cols
                info["errors"].append("字段不存在: " + ", ".join(unknown_cols))

        # 聚合字段校验
        select_expr = parsed.find(exp.Select)
        aggregated_columns: set[str] = set()
        non_aggregated_columns: set[str] = set()
        uses_aggregate = False
        if select_expr is not None:
            for proj in select_expr.expressions:
                has_aggregate = False
                for func in proj.find_all(exp.Func):
                    fname = self._normalize_identifier(getattr(func, "name", None))
                    if fname and fname in self.AGGREGATE_FUNCTIONS:
                        has_aggregate = True
                        uses_aggregate = True
                target_columns = {
                    self._normalize_identifier(getattr(col, "name", None))
                    for col in proj.find_all(exp.Column)
                    if getattr(col, "name", None)
                }
                target_columns = {c for c in target_columns if c and c != "*"}
                if has_aggregate:
                    aggregated_columns.update(target_columns)
                else:
                    non_aggregated_columns.update(target_columns)

            group_expr = select_expr.args.get("group")
            group_columns: set[str] = set()
            if isinstance(group_expr, exp.Group):
                for g in group_expr.expressions:
                    for col in g.find_all(exp.Column):
                        name = self._normalize_identifier(getattr(col, "name", None))
                        if name and name != "*":
                            group_columns.add(name)
            info["group_by_columns"] = sorted(group_columns)
            info["aggregated_columns"] = sorted(aggregated_columns)
            info["non_aggregated_columns"] = sorted(non_aggregated_columns)

            if uses_aggregate and non_aggregated_columns:
                diff = sorted(non_aggregated_columns - group_columns)
                if diff:
                    info["valid"] = False
                    info["errors"].append(
                        "存在未分组字段: " + ", ".join(diff)
                    )

        expected_table = self._normalize_identifier((schema or {}).get("table"))
        if expected_table and expected_table not in tables:
            info["valid"] = False
            info["errors"].append(f"SQL 未引用目标表 `{expected_table}`")

        return info

    # ----------------------------------
    # 语义 LLM 校验（含 SQL→NL）
    # ----------------------------------
    def _semantic_alignment(
        self,
        user_query: str,
        sql_generated: str,
        schema: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
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
            "你是 MySQL 专家，负责判断 SQL 是否满足用户需求，并把 SQL 转写为自然语言解释。\n"
            "请输出 JSON，键包括：\n"
            "semantic_valid (bool)、semantic_reason (string)、sql_nl_explanation (string)、fix_suggestion (string)、confidence (0-1)。"
        )
        user_prompt = (
            f"用户问题: {user_query}\n"
            f"SQL: {sql_generated}\n"
            f"表结构:\n{schema_text}\n"
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
    # 嵌入相似度（Bag-of-Words）
    # ----------------------------------
    def _embedding_similarity(self, query: str, sql_nl: str) -> Dict[str, Any]:
        tokens_a = self._tokenize(query)
        tokens_b = self._tokenize(sql_nl)
        info: Dict[str, Any] = {
            "valid": False,
            "score": 0.0,
            "threshold": self.similarity_threshold,
            "errors": [],
        }
        if not tokens_a or not tokens_b:
            info["errors"].append("文本不足，无法计算相似度")
            return info

        vec_a = self._vectorize(tokens_a)
        vec_b = self._vectorize(tokens_b)
        score = self._cosine(vec_a, vec_b)
        info["score"] = score
        if score >= self.similarity_threshold:
            info["valid"] = True
        else:
            info["errors"].append(
                f"相似度过低（{score:.2f} < {self.similarity_threshold:.2f}）"
            )
        return info

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
    @staticmethod
    def _normalize_identifier(value: Optional[str]) -> str:
        if not value:
            return ""
        s = str(value)
        s = s.replace("`", "")
        if "." in s:
            s = s.split(".")[-1]
        return s.strip().lower()

    @staticmethod
    def _tokenize(text: str) -> List[str]:
        if not text:
            return []
        return re.findall(r"[\w]+", text.lower())

    @staticmethod
    def _vectorize(tokens: Iterable[str]) -> Dict[str, float]:
        vec: Dict[str, float] = {}
        total = 0
        for token in tokens:
            vec[token] = vec.get(token, 0.0) + 1.0
            total += 1
        if total:
            for k in list(vec.keys()):
                vec[k] /= total
        return vec

    @staticmethod
    def _cosine(vec_a: Dict[str, float], vec_b: Dict[str, float]) -> float:
        keys = set(vec_a) | set(vec_b)
        dot = sum(vec_a.get(k, 0.0) * vec_b.get(k, 0.0) for k in keys)
        norm_a = math.sqrt(sum(v * v for v in vec_a.values()))
        norm_b = math.sqrt(sum(v * v for v in vec_b.values()))
        if not norm_a or not norm_b:
            return 0.0
        return dot / (norm_a * norm_b)

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

    def _normalize_result(self, obj: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "valid": bool(obj.get("valid", False)),
            "reason": str(obj.get("reason", "")),
            "fix_suggestion": str(obj.get("fix_suggestion", "")),
            "need_regenerate": bool(obj.get("need_regenerate", not bool(obj.get("valid", False))))
        }
    

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

    def run(
        self,
        user_query: str,
        sql_generated: str,
        schema: Optional[Dict[str, Any]] = None,
        db: Any = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        # 兼容旧调用：某些地方可能通过关键字参数传入 schema/db
        if schema is None and "schema" in kwargs:
            schema = kwargs.get("schema")
        if db is None and "db" in kwargs:
            db = kwargs.get("db")

        try:
            syntax_info = self._check_syntax(sql_generated, schema)
            semantic_info = self._semantic_alignment(user_query, sql_generated, schema)
            explanation = semantic_info.get("sql_nl_explanation", "")
            sql2nl_info = {
                "valid": bool(explanation),
                "explanation": explanation,
                "errors": [] if explanation else ["未生成 SQL 自然语言解释"],
            }
            embedding_info = self._embedding_similarity(user_query, explanation or sql_generated)
            execution_info = self._execution_check(sql_generated, db)

            combined_errors = self._aggregate_errors(
                (syntax_info, "语法校验失败"),
                (semantic_info, "语义判定失败"),
                (sql2nl_info, "SQL→自然语言解释缺失"),
                (embedding_info, "语义相似度不足"),
                (execution_info, "SQL 无法执行"),
            )

            valid = (
                syntax_info.get("valid")
                and semantic_info.get("valid")
                and sql2nl_info.get("valid")
                and embedding_info.get("valid")
                and execution_info.get("valid")
            )

            reason = combined_errors[0] if combined_errors else "SQL 校验通过"
            fix_candidates: List[str] = []
            for item in (semantic_info, syntax_info, execution_info):
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
                "semantic_similarity": float(embedding_info.get("score", 0.0)),
                "need_regenerate": not bool(valid),
                "details": {
                    "syntax": syntax_info,
                    "semantic": semantic_info,
                    "sql2nl": sql2nl_info,
                    "embedding": embedding_info,
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
                "semantic_similarity": 0.0,
                "need_regenerate": True,
                "details": {},
            }

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

    def run(self, user_query: str, sql_generated: str) -> Dict[str, Any]:
        if not self.llm_assistant:
            # 无 LLM 时，保守返回“需重试”
            return {
                "valid": False,
                "reason": "判别模型未配置",
                "fix_suggestion": "请检查 SQL 字段/分组/排序是否与需求一致，并添加 LIMIT 以避免全表扫描",
                "need_regenerate": True,
            }
        try:
            system_prompt = self._build_system_prompt()
            user_prompt = self._build_user_prompt(user_query, sql_generated)
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]
            text = self._get_last_text(self.llm_assistant, messages)
            obj = self._parse_json(text)
            if not obj:
                return {
                    "valid": False,
                    "reason": "判别模型未返回有效 JSON",
                    "fix_suggestion": "请检查 SQL 的 GROUP BY/HAVING/WHERE、字段存在性与语义一致性",
                    "need_regenerate": True,
                }
            return self._normalize_result(obj)
        except Exception as e:
            self._logger.exception("SqlJudgeAgent 失败")
            return {
                "valid": False,
                "reason": f"判别异常: {e}",
                "fix_suggestion": "请缩小时间范围或明确分组/排序口径后重试",
                "need_regenerate": True,
            }

    
