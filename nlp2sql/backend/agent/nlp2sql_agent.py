
from __future__ import annotations

from typing import Any, Dict, List, Optional
import logging
import json
from .base_agent import BaseAgent

class Nlp2SqlAgent(BaseAgent):
    """
    将自然语言转 SQL 的 Agent。
    契约：
        run(user_nl: str, database: str, table: str, conn: Any) -> str
    特性：
        - 从数据库获取表结构与样例数据，作为上下文
        - 调用 llm_assistant 生成 SQL（仅一条语句，末尾分号）
        - 兼容 DataAnalysisAgent 的流式输出处理
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.llm_assistant = kwargs.get("llm_assistant")
        self.max_preview_rows: int = int(kwargs.get("max_preview_rows", 20))
        self._logger = logging.getLogger(self.__class__.__name__)
        # 存储最近一次规划/生成/执行的上下文，便于外部调试或记录
        self.last_plan: List[str] | None = None
        self.last_sql_sequence: List[str] | None = None
        self.last_execution_results: List[Any] | None = None
        self._last_step_context: List[Dict[str, Any]] | None = None

    #流式输出结果获取处理
    def _get_last_llm_output(self, assistant, messages, stream: bool = True) -> str:
        sql_text = ""
        DELIM = "\n<CHUNK_END>\n"
        try:
            for chunk in assistant.run(messages=messages, stream=stream):
                if isinstance(chunk, list):
                    for item in chunk:
                        if isinstance(item, dict):
                            part = item.get("content", "") or item.get("reasoning_content", "")
                            if part:
                                sql_text += part + DELIM
                        elif isinstance(item, str):
                            sql_text += item + DELIM
                elif isinstance(chunk, dict):
                    part = chunk.get("content", "") or chunk.get("reasoning_content", "")
                    if part:
                        sql_text += part + DELIM
                elif isinstance(chunk, str):
                    sql_text += chunk + DELIM
                else:
                    try:
                        for item in chunk:
                            if isinstance(item, dict):
                                part = item.get("content", "") or item.get("reasoning_content", "")
                                if part:
                                    sql_text += part + DELIM
                            elif isinstance(item, str):
                                sql_text += item + DELIM
                    except Exception:
                        pass
                # print("*"*30)
                # print(chunk)

            # 取最后一个非空 chunk 作为最终输出
            if DELIM in sql_text:
                parts = [p.strip() for p in sql_text.split(DELIM) if p.strip()]
                result = parts[-1] if parts else ""
            else:
                result = sql_text.strip()
            
            # 如果包含分号，截取到最后一个分号（包含分号）
            if ";" in result:
                idx = result.rfind(";")
                result = result[: idx + 1].strip()
            return result
        
        except Exception as e:
            print(f"get_last_llm_output 错误: {e}")
            return ""

    def _get_last_text_output(self, assistant, messages, stream: bool = True) -> str:
        """与 _get_last_llm_output 类似，但不做分号裁剪，适用于纯文本/NL 场景。"""
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
        except Exception as e:
            print(f"get_last_text_output 错误: {e}")
            return ""

    def _normalize_rows(self, rows: Any, col_names: Optional[List[str]]) -> List[Dict[str, Any]]:
        """把查询结果统一为 list[dict] 方便后续处理。"""
        if not rows:
            return []
        out: List[Dict[str, Any]] = []
        if isinstance(rows, list):
            first = rows[0]
            if isinstance(first, dict):
                return rows  # 已经是字典
            if isinstance(first, (list, tuple)):
                # 使用列名映射
                if not col_names:
                    # 没有列名，只能转为 idx: value
                    for r in rows:
                        out.append({str(i): v for i, v in enumerate(r)})
                else:
                    for r in rows:
                        out.append({col_names[i]: r[i] for i in range(min(len(r), len(col_names)))})
                return out
        # 其他类型兜底
        for r in rows:
            try:
                out.append(dict(r))
            except Exception:
                out.append({"value": str(r)})
        return out

    def _fetch_schema(self, conn: Any, database: str, table: str) -> Dict[str, Any]:
        """获取表结构信息"""
        sql = f"SHOW FULL COLUMNS FROM `{database}`.`{table}`;"
        rows = conn.execute_query(sql)
        col_names = None
        try:
            col_names = conn.get_column_names(sql)
        except Exception:
            pass
        items = self._normalize_rows(rows, col_names)
        cols: List[Dict[str, Any]] = []
        for it in items:
            cols.append({
                "name": it.get("Field") or it.get("COLUMN_NAME") or it.get("field") or it.get("column_name") or "",
                "type": it.get("Type") or it.get("COLUMN_TYPE") or it.get("type") or it.get("column_type") or "",
                "nullable": (it.get("Null") or it.get("IS_NULLABLE") or "").upper() in ("YES", "Y", "TRUE"),
                "key": it.get("Key") or it.get("COLUMN_KEY") or "",
                "default": it.get("Default") if "Default" in it or "default" in it else None,
                "comment": it.get("Comment") or it.get("COLUMN_COMMENT") or "",
            })
        return {
            "database": database,
            "table": table,
            "columns": [c for c in cols if c["name"]],
        }

    def _fetch_column_distincts(self, conn: Any, database: str, table: str, columns: List[Dict[str, Any]], limit: int = 10) -> Dict[str, Dict[str, Any]]:
        """
        对每个字段，获取前limit个distinct值，并判断是否有约束。
        返回: {col: {"distinct": [...], "constrained": bool}}
        """
        result = {}
        for col in columns:
            colname = col["name"]
            sql = f"SELECT DISTINCT `{colname}` FROM `{database}`.`{table}` WHERE `{colname}` IS NOT NULL LIMIT {limit+1};"
            try:
                rows = conn.execute_query(sql)
                vals = []
                if rows:
                    if isinstance(rows[0], dict):
                        vals = [list(r.values())[0] for r in rows]
                    elif isinstance(rows[0], (list, tuple)):
                        vals = [r[0] for r in rows]
                constrained = len(vals) <= limit
                result[colname] = {"distinct": vals[:limit], "constrained": constrained}
            except Exception:
                result[colname] = {"distinct": [], "constrained": False}
        return result

    def _build_system_prompt(self) -> str:
        system_prompt = (
                "你是一个资深的 SQL 助手，擅长将用户的自然语言问题精确地转换为 MySQL 查询语句。\n"
                "\n"
                "=============================\n"
                "【输出要求】\n"
                "=============================\n"
                "1) 仅输出一条 SQL 语句，必须以 SELECT 开头，并以分号结尾；禁止输出任何解释、注释或多余文本。\n"
                "2) 输出语句严格符合MySQL语法\n"
                "3) 对齐已知的数据库/表/列名，禁止使用不存在的字段。\n"
                "4) 标识符统一使用反引号包裹，如 `db`.`table`、`col`。\n"
                "\n"
                "=============================\n"
                "【通用生成规则】\n"
                "=============================\n"
                "1) LIMIT 安全：禁止无限制查询；若用户未指定返回条数，统一追加 LIMIT 1000。\n"
                "2) 聚合与分组：\n"
                "   - 若查询涉及聚合（SUM/AVG/COUNT）且用户语义含“各/各个/按/每个/分别/不同/每家”等词，必须包含 GROUP BY。\n"
                "   - SELECT 中必须同时输出分组字段与聚合字段。\n"
                "3) 精度与取整：\n"
                "   - AVG、SUM、比例类字段默认使用 ROUND(..., 2)。\n"
                "4) 空值健壮性：\n"
                "   - 对空值使用 COALESCE()/IFNULL()，除法时用 NULLIF(分母, 0)。\n"
                "5) 时间计算：\n"
                "   - 时间计算规则（TIMESTAMPDIFF / DATEDIFF）：在计算工龄、住院天数、使用时长等时间区间时，必须使用 TIMESTAMPDIFF() 或 DATEDIFF()。"
                "   对结束日期为空的情况，不得一律替换为 CURDATE()；需遵循以下逻辑："
                "   - 若空值表示“仍在进行中”（如 leave_date、discharge_date、usage_end），或 status ∈ ('active','in_progress','ongoing')，则可用 CURDATE() 替代。"
                "   - 若空值仅表示“数据缺失”“尚未录入”或属于历史快照记录，则不可使用 CURDATE()，应保留 NULL 或过滤掉。\n"
                "6) 排序与排名：\n"
                "   - “最高/最大/Top/前N/排名前” → ORDER BY 指标 DESC；“最低/最小/后N” → ASC。\n"
                "   - “每个X中最高/最大” 表示分组内极值，必须使用窗口函数或子查询。\n"
                "   - 如果存在相同指标值导致并列排名，应全部返回，即需要通过比较最大/最小值回溯所有并列记录（如使用子查询或 WHERE 指标 = 全局最大值）。不能只取固定 N 条。  \n"
                "7) 字段选择：尽量明确列出所需字段，禁止 SELECT *。\n"
                "8) 聚合过滤：聚合后条件使用 HAVING，行级过滤使用 WHERE。\n"
                "9) 时间序列：出现“按日/周/月/季度/年/趋势”时，应显式输出时间字段并设聚合粒度。\n"
                "10) 比例与占比：出现“占比/比例/份额/贡献度”时，使用 (分子 / NULLIF(分母, 0)) * 100 并 ROUND(..., 2)。\n"
                "\n"
                "=============================\n"
                "【高级逻辑与歧义消解 (A-G)】\n"
                "=============================\n"
                "A) 分组意图判定：\n"
                "   - 若自然语言包含“各/各个/每个/每家/每类/按/分/分别/不同”等词，表示要对分组聚合；SQL 必须包含 GROUP BY。\n"
                "   - 若问题中出现“...中...最高.../...中...最低.../...中...最大...”类似的结构   ，必须理解为“每组内取极值”的问题，SQL 需使用窗口函数（ROW_NUMBER/RANK）或子查询 + MAX/MIN 聚合方式取组内第一。\n"
                "\n"
                "B) 同义歧义归一：\n"
                "   - “每个”“各个”“各”“每家”“不同”“分别”“按X分”语义相同 → 都表示分组。\n"
                "   - “中最高/中最小”语义 ≠ “最高/最小”；前者是组内极值，后者是全局极值。\n"
                "   - “每个医院中最高” → 按医院分组取平均工资最高的记录。\n"
                "   - “各个医院中最高” → 与上同义，不得误判为全局排序。\n"
                "\n"
                "C) 极值判断与SQL生成：\n"
                "   - 若语义为“每组内取最大/最小”→ 必须生成：\n"
                "       ① 使用窗口函数 ROW_NUMBER()/RANK() OVER(PARTITION BY group_field ORDER BY metric DESC/ASC) + 外层 WHERE rank=1；或\n"
                "       ② 使用双层子查询 + MAX()/MIN() 匹配方案。\n"
                "   - 若语义为“全局最大/最小”→ 仅使用 ORDER BY + LIMIT 1。\n"
                "\n"
                "D) 时间与趋势分析：\n"
                "   - “按日/月/年/趋势” → 时间字段需聚合或分组。\n"
                "   - “近N天/去年/本月” → 构造时间过滤条件。\n"
                "\n"
                "E) 数值精度与比例：\n"
                "   - 保留两位小数 (ROUND(...,2))，防止精度丢失。\n"
                "\n"
                "F) 状态与否定语义：\n"
                "   - “尚未/未/没有” → IS NULL 或 =0；“已/存在” → IS NOT NULL 或 >0；“进行中” → start_date <= CURDATE() AND (end_date IS NULL OR end_date >= CURDATE())。\n"
                "\n"
                "G) 窗口函数触发逻辑：\n"
                "   - 若句子包含“每个X中最高/最大/最小/TopN”等结构，必须优先考虑使用窗口函数（ROW_NUMBER/RANK/DENSE_RANK）进行分组排名。\n"
                "   - 若 MySQL 版本支持窗口函数（≥8.0），优先使用窗口函数；否则使用子查询 + 聚合替代。\n"
                "\n"
                "（以上 A-G 七项仅用于模型内部推理，不得出现在最终 SQL 输出中。）\n"
                "\n"
                "=============================\n"
                "【最终输出要求】\n"
                "=============================\n"
                "输出仅包含一条完整的 MySQL SELECT 语句，不得附带任何解释、注释或分析性文字。"
            )

        return system_prompt


    def _build_user_prompt(self, user_nl: str, schema: Dict[str, Any], col_distincts: Dict[str, Dict[str, Any]], fix_suggestion: str | None = None) -> str:
        schema_text = json.dumps(schema, ensure_ascii=False, indent=2, default=str)
        # 拼接每个字段的distinct信息
        distinct_lines = []
        for col in schema["columns"]:
            name = col["name"]
            d = col_distincts.get(name, {})
            vals = d.get("distinct", [])
            constrained = d.get("constrained", False)
            if vals:
                val_str = ", ".join([str(v) for v in vals])
                if constrained:
                    distinct_lines.append(f"字段 `{name}` 约束值: {val_str} (仅可选其一)")
                else:
                    distinct_lines.append(f"字段 `{name}` 前10个不同值: {val_str} (内容无约束)")
        distinct_text = "\n".join(distinct_lines)
        extra = ""
        if fix_suggestion:
            extra = f"\n\n修复提示（请严格遵循）：{fix_suggestion}"
        return (
            f"用户需求：{user_nl}\n\n"
            f"数据表结构（来自 {schema['database']}.{schema['table']}）：\n{schema_text}\n\n"
            f"字段约束信息：\n{distinct_text}" + extra + "\n\n"
            "请基于上述信息，生成一条满足需求的 MySQL 查询语句。仅输出 SQL。"
        )


    def _postprocess_sql(self, sql: str, database: str, table: str) -> str:
        # 最后的 SQL 后处理，确保符合要求
        if not sql:
            return ""
        sql = sql.strip()

        # 如果没有分号，补上
        if not sql.endswith(";"):
            sql += ";"

        # # 如果没有 LIMIT，自动补 LIMIT 1000
        # lower = sql.lower()
        # if " limit " not in lower:
        #     sql = sql[:-1] + " LIMIT 1000;"

        # 确保引用目标表（尽量避免被 LLM 换表），若未包含库名，则补齐
        target = f"`{database}`.`{table}`"
        if f"`{table}`" in sql and f"`{database}`.`{table}`" not in sql:
            sql = sql.replace(f"`{table}`", target)

        return sql
    
    def _plan_subqueries(self, user_nl: str) -> List[str]:
        """判断是否需要拆解为子问题，返回子问题列表。"""
        if not user_nl:
            return []
        if self.llm_assistant is None:
            return [user_nl]
        system_prompt = (
            "你是一名SQL问题规划专家。\n"
            "任务：判断用户的问题是否需要拆分，并在需要时生成一组子问题，帮助逐步求解。\n"
            "要求：\n"
            "1) 输出必须是 JSON，对象包含 need_split (bool) 与 sub_queries (list[str])。\n"
            "2) 当问题无需拆分时，sub_queries 只包含原问题；need_split 为 false。\n"
            "3) 当需要拆分时，sub_queries 列表按执行顺序排列，最后一个子问题必须直接回答原始问题。\n"
            "4) 不得添加表名或臆造字段，只能改写或细化原始语义。\n"
            "5) JSON 之外不得包含任何说明或标点。"
        )
        user_prompt = (
            f"原始用户问题：{user_nl}\n"
            "请输出满足上述要求的 JSON。"
        )
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        raw = self._get_last_text_output(self.llm_assistant, messages, stream=True)
        text = (raw or "").strip()
        if text.startswith("```") and text.endswith("```"):
            text = text.strip("`")
        text = text.strip()
        try:
            data = json.loads(text)
            steps = data.get("sub_queries") or data.get("steps") or []
            if isinstance(steps, list):
                cleaned = [str(s).strip() for s in steps if str(s).strip()]
            else:
                cleaned = []
            need_split = bool(data.get("need_split"))
            if cleaned:
                if need_split and len(cleaned) >= 2:
                    return cleaned
                if not need_split:
                    return cleaned
        except Exception as exc:  # pragma: no cover - 容错
            self._logger.debug("plan_subqueries 解析失败，回退原问题: %s", exc)
        return [user_nl]
    
    def _generate_sql_for_query(
        self,
        user_nl: str,
        database: str,
        table: str,
        schema: Dict[str, Any],
        col_distincts: Dict[str, Dict[str, Any]],
        system_prompt: str,
        fix_suggestion: str | None = None,
    ) -> str:
        user_prompt = self._build_user_prompt(user_nl, schema, col_distincts, fix_suggestion=fix_suggestion)
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
        sql = self._get_last_llm_output(self.llm_assistant, messages, stream=True)
        if isinstance(sql, dict) and "content" in sql:
            sql = sql["content"]
        return self._postprocess_sql(sql, database, table)

    def run(
        self,
        user_nl: str,
        database: str,
        table: str,
        conn: Any,
        fix_suggestion: str | None = None,
        execute: bool = True,
    ) -> Any:
        """
        主入口：

        - 默认返回最终 SQL 字符串。
        - 当 execute=True 时，返回包含规划、SQL 序列与执行结果的字典。
        """
        if not user_nl or not database or not table or conn is None:
            return "" if not execute else {"plan": [], "sql_sequence": [], "results": [], "final_result": None}

        plan = self._plan_subqueries(user_nl)
        if not plan:
            plan = [user_nl]

        schema = self._fetch_schema(conn, database, table)
        col_distincts = self._fetch_column_distincts(conn, database, table, schema["columns"], limit=10)
        system_prompt = self._build_system_prompt()

        sql_sequence: List[str] = []
        execution_results: List[Any] = []

        for idx, sub_query in enumerate(plan):
            fix = fix_suggestion if idx == len(plan) - 1 else None
            sql = self._generate_sql_for_query(sub_query, database, table, schema, col_distincts, system_prompt, fix_suggestion=fix)
            sql_sequence.append(sql)
            if execute:
                try:
                    result = conn.execute_query(sql)
                except Exception as exc:  # pragma: no cover - 数据库异常容错
                    self._logger.warning("执行 SQL 失败: %s", exc)
                    result = None
                execution_results.append(result)

        self.last_plan = plan
        self.last_sql_sequence = sql_sequence
        self.last_execution_results = execution_results if execute else None

        if execute:
            final_result = execution_results[-1] if execution_results else None
            return {
                "plan": plan,
                "sql_sequence": sql_sequence,
                "results": execution_results,
                "final_result": final_result,
            }

        return sql_sequence[-1] if sql_sequence else ""