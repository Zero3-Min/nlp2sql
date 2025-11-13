from __future__ import annotations
from typing import Any, Dict
import json
import logging
import re

from .base_agent import BaseAgent


class SqlJudgeAgent(BaseAgent):
    """
    SQL 判别专家：判定 text_to_sql_agent 生成的 SQL 是否正确并给出修复建议。

    输入：user_query(str), sql_generated(str)
    输出 JSON：
    {
      "valid": true/false,
      "reason": "语法错误或语义不符的具体原因",
      "fix_suggestion": "应该如何修改（提示给 text_to_sql_agent）",
      "need_regenerate": true/false
    }
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.llm_assistant = kwargs.get("llm_assistant")
        self._logger = logging.getLogger(self.__class__.__name__)

    def _build_system_prompt(self) -> str:
        return (
            "你是资深 SQL 评审专家。\n"
            "任务：对给定的 MySQL 查询进行全面判定，并输出严格的 JSON 结论。\n"
            "检查范围：\n"
            "- 语法合法性（MySQL 8.0）；\n"
            "- 字段/表是否存在（根据用户描述与常识判定，如无法确认需说明不确定点）；\n"
            "- WHERE/HAVING/GROUP BY 的使用是否正确；\n"
            "- 是否与用户查询语义一致（聚合/分组/排序/TopN/时间范围/口径/单位/小数位等）。\n"
            "输出要求：\n"
            "- 仅输出 JSON，不要任何解释；\n"
            "- JSON 键为：valid(boolean), reason(string), fix_suggestion(string), need_regenerate(boolean)；\n"
            "- 若 SQL 可通过但存在改进空间，valid=true 且给出 fix_suggestion（可为空）。\n"
        )

    def _build_user_prompt(self, user_query: str, sql_generated: str) -> str:
        return (
            f"用户问题：{user_query}\n"
            f"候选 SQL：\n{sql_generated}\n\n"
            "请输出严格 JSON，例如：\n"
            "{\n"
            "  \"valid\": false,\n"
            "  \"reason\": \"WHERE 中字段不存在：user_id\",\n"
            "  \"fix_suggestion\": \"请改用真实主键 id，并在 SELECT 中明确列出所需字段；若要分组统计，请补 GROUP BY\",\n"
            "  \"need_regenerate\": true\n"
            "}\n"
        )

    def _parse_json(self, text: str) -> Dict[str, Any]:
        """尽量从模型输出中解析 JSON。"""
        s = (text or "").strip()
        # 去除 ```json 包裹
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
        # 兜底：从文本中粗略提取 {...}
        try:
            start = s.find('{')
            end = s.rfind('}')
            if start != -1 and end != -1 and end > start:
                obj = json.loads(s[start:end+1])
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

    
