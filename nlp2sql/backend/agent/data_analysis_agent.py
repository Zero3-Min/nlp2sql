from __future__ import annotations
from typing import Any, Dict, Literal
from matplotlib.style import context
import pandas as pd
import logging
from pandas.api.types import is_datetime64_any_dtype
import json

from .base_agent import BaseAgent
from tool.create_chart import create_chart, ChartType
from tool.create_table import create_table
from tool.data_summary import data_summary


class DataAnalysisAgent(BaseAgent):
    """基于 SQL 结果 DataFrame 的分析与可视化 Agent。

    契约：
        run(df: pd.DataFrame, user_nl: str) -> str
    产物：
        - 在当前工作目录生成 `<chart_xxx.json>` 或 `<table_xxx.json>`
        - 返回包含文件名引用与基础洞察的文本
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        # 工具映射，可被外部注入覆盖
        self.tools = {
            'data_summary': kwargs.get('data_summary', data_summary),
            'create_table': kwargs.get('create_table', create_table),
            'create_chart': kwargs.get('create_chart', create_chart),
        }
        self.llm_assistant = kwargs.get('llm_assistant')
        # 数据分析可视化文件保存
        self.output_dir = kwargs.get('output_dir') or "/home/minshunhua/data/P1/vanna/src/nlp2sql/backend/temp_data"
        self.max_points: int = int(kwargs.get('max_points', 1000))
        # 模式注册表
        self.mode_registry: Dict[str, str] = {}
        # 注册默认模板
        self._register_default_modes()
        self._logger = logging.getLogger(self.__class__.__name__)

    # ---------- 内部能力 ----------
    def get_last_llm_output(self, assistant, messages, stream=True) -> str:
        """
        获取大模型返回内容的最后一句文本。
        :param assistant: Assistant 实例
        :param messages: 输入消息列表
        :param stream: 是否流式
        :return: 最后一句输出文本
        """
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
        
    # 注册模式模板
    def register_mode(self, mode: str, system_prompt: str):
        """注册新的分析模式提示词"""
        self.mode_registry[mode] = system_prompt

    def _register_default_modes(self):
        """预注册 chart / report 模式"""
        self.mode_registry["chart"] = """
                你是一位专业的数据可视化专家。你的任务是根据用户的具体需求，快速生成指定的图表，并提供简洁而精准的数据洞察分析。

                ## 工作流程
                1. 根据数据结构和基本情况，快速了解数据。
                2. 结合用户需求，使用提取和计算相关数据的特征。
                3. 通过图表分析数据，并提供简短而有价值的数据洞察。

                ## 输出模板
                **图表说明**: [简要说明生成的图表类型和展示内容]

                <[图表文件名，如 bar_chart_xxx.json]>

                **图表分析**: [分析主要结论和建议]
                """

        self.mode_registry["report"] = """
                你是一位资深的数据科学家。你的核心任务是生成一份结构清晰、富有洞察力的深度分析报告。

                ## 分析逻辑
                1. 构思分析规划；
                2. 按维度生成图表；
                3. 严格引用真实文件路径；
                4. 结合数据与图表输出完整报告。

                ## 输出模板
                # [报告标题]
                ## 1. 摘要
                [概述核心发现]
                ## 2. 指标概览
                <[表格文件]>
                ## 3. 趋势与对比
                <[图表文件]>
                **洞察**: [主要发现]
                ## 4. 结论与建议
                - [结论1]
                - [建议1]
                """
    # 构建数据架构概览
    def _schema_overview(self, df: pd.DataFrame) -> dict:
        """构建列的类型与基础统计，用于传给 LLM 做决策。"""
        from pandas.api.types import is_numeric_dtype
        rows = []
        for c in df.columns:
            try:
                s = df[c]
                row = {
                    "name": str(c),
                    "dtype": str(s.dtype),
                    "is_datetime": bool(is_datetime64_any_dtype(s)),
                    "is_numeric": bool(is_numeric_dtype(s)),
                    "nunique": int(s.nunique(dropna=True))
                }
                rows.append(row)
            except Exception:
                continue

        return {"columns": rows, "rows": int(len(df))}

    #与LLM交互，产出可视化计划
    def _llm_plan_chart(self, summary_text: str, schema: dict, user_nl: str) -> dict | list[dict] | None:
        """让 LLM 产出可视化图（严格 JSON）。

        返回：
        - dict: 单图计划；
        - None: 计划失败或未配置 LLM。
        计划键包含：chart_type/x/y/agg/freq/title/x_label/y_label/notes
        """
        if self.llm_assistant is None:
            return None
        plan_schema = (
            "你将根据用户需求、数据摘要和数据架构，产出一个严格的图表报告的生成计划。\n"
            "允许输出：\n- 单个对象（单张图）\n"
            "每个对象包含键：\n"
            "- chart_type: 'bar' | 'line' | 'pie' | 'table'\n"
            "- x: 作为横轴或分类的列名（没有则为 null）\n"
            "- y: 作为数值度量的列名（没有则为 null；当 chart_type 为 'pie'/'bar' 时通常需要）\n"
            "- agg: 'sum' | 'mean' | 'count' | 'max' | 'min'（若需要聚合）\n"
            "- freq: 'D' | 'W' | 'M' | 'Q' | 'Y'（仅当 x 为时间列且需要重采样时）\n"
            "- title: 图表标题\n"
            "- x_label: 横轴标题\n"
            "- y_label: 纵轴标题\n"
            "- notes: 一两句简短说明\n"
            "仅输出 JSON（对象或对象数组），不要任何多余文本。"
        )
        context = (
            f"用户需求: {user_nl}\n\n"
            f"数据摘要:\n{summary_text}\n\n"
            f"数据架构:\n{json.dumps(schema, ensure_ascii=False)}\n"
        )

        messages = [
            {"role": "system", "content": plan_schema},
            {"role": "user", "content": context},
        ]
        try:

            text = self.get_last_llm_output(self.llm_assistant, messages, stream=True)

            # 尝试解析为 JSON 计划
            if text:
                try:
                    return json.loads(text)
                except Exception:
                    self._logger.debug(text)
                    self._logger.warning("LLM 计划返回不是有效 JSON")
                    return None

        except Exception as e:
            self._logger.exception("LLM可视化计划失败")
            return None
        return None
    
    def _chart_from_plan(self, df, plan) -> str:
        """将 plan(JSON) 直接传入 create_chart，返回生成的首个文件路径字符串。"""
        plans = plan if isinstance(plan, list) else [plan]
        out_files: list[str] = []
        for p in plans:
            if not isinstance(p, dict):
                continue
            ct = ChartType(str(p.get('chart_type', 'bar')).lower())
            x = p.get('x')
            y = p.get('y')
            fpath = self.tools['create_chart'](
                df=df,
                x=x,
                y=y,
                chart_type=ct,
                output_dir=self.output_dir,
                max_points=self.max_points,
                save_image=True,
                title=p.get('title'),
                x_label=p.get('x_label'),
                y_label=p.get('y_label'),
                description=p.get('notes'),
            )
            if isinstance(fpath, str):
                out_files.append(fpath)
        return out_files[0] if out_files else ""


    # ---------- 对外主流程 ----------
    def run(self, df: pd.DataFrame | Any, user_nl: str, mode: str = "chart") -> str:
        """
        多模式执行函数：chart / report / forecast / ...
        """
        # 自动判断模式
        if mode not in self.mode_registry:
            # 自动识别（智能匹配）
            if any(k in user_nl for k in ["报告", "分析", "总结", "原因"]):
                mode = "report"
            else:
                mode = "chart"

        # === ① 数据摘要 ===
        try:
            summary_text = self.tools['data_summary'](df)
        except Exception as e:
            summary_text = f"数据预览失败: {e}"

        # === ② 可视化规划 ===
        plan = None
        try:
            plan = self._llm_plan_chart(summary_text, self._schema_overview(df), user_nl)
        except Exception as e:
            print(f"可视化计划生成失败: {e}")

        # === ③ 表格文件 ===
        table_file = None
        try:
            table_file = self.tools['create_table'](
                df.head(100), output_dir=self.output_dir, title='结果预览'
            )
        except Exception as e:
            print(f'表格生成失败: {e}')

        # # === ④ 图表文件 ===
        # chart_file = None
        # try:
        #     chart_file = self._chart_from_plan(df, plan)
        # except Exception as e:
        #     print(f'图表生成失败: {e}')

        # # === ⑤ LLM 报告生成 ===
        # parts: list[str] = []
        # parts.append(f'【执行模式】{mode}\n')
        # parts.append('数据摘要:\n' + summary_text + "\n")

        # system_prompt = self.mode_registry.get(mode, "")
        # context = [
        #     f"用户需求：{user_nl}\n",
        #     f"数据摘要：{summary_text}\n"
        # ]
        # if plan:
        #     context.append(f"可视化计划：{plan}\n")
        # if table_file:
        #     context.append(f"表格文件：<{table_file}>\n")
        # if chart_file:
        #     context.append(f"图表文件：<{chart_file}>\n")

        # messages = [
        #     {"role": "system", "content": system_prompt},
        #     {"role": "user", "content": "".join(context)},
        # ]

        # if self.llm_assistant is not None:
        #     try:
        #         resp = self.get_last_llm_output(
        #             self.llm_assistant, messages, stream=True
        #         )
        #         if resp:
        #             parts.append(resp)
        #     except Exception as e:
        #         parts.append(f'\n[LLM 报告生成失败: {e}]')

        # return "\n".join(parts)
        return "!!!!!!!!!!None is None!!!!!!!!!!"


