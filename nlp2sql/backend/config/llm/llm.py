"""
LLM配置和初始化模块
用于配置Qwen3:32B模型和初始化Agent
"""
import json, datetime
import os
from qwen_agent.agents import Assistant
from qwen_agent.llm import get_chat_model
from typing import List, Dict, Any
from agent.agent_manager import AgentManager
from dotenv import load_dotenv

class LLM:
    def __init__(self, **kwds ):
        self.model_name = kwds.get("model_name", "qwen3-32B")
        self.model_server = kwds.get("model_server", "http://18.145.150.40:9033")   
        self.api_key = kwds.get("api_key", "key")
        self.enable_thinking = kwds.get("enable_thinking", False)
        self.max_input_tokens = kwds.get("max_input_tokens", 32000)
        self.temperature = kwds.get("temperature", 0.7)
        self.top_p = kwds.get("top_p", 0.8)
        self.top_k = kwds.get("top_k", 20)
        self.tools = [
            # 'create_chart',   # 图表生成工具
            # 'create_table',   # 表格生成工具
            # 'data_summary'   # 数据概览工具
        ]
        self.llm_cfg = {
            'model': self.model_name,
            'model_server': self.model_server,
            'api_key': self.api_key,
            'generate_cfg': {
                # 针对vLLM/SGLang OAI API的思考模式配置
                'extra_body': {
                    'chat_template_kwargs': {'enable_thinking': self.enable_thinking}
                },
                # 当内容格式为 `<think>this is the thought</think>this is the answer` 时设为True
                # 当响应已被分离为reasoning_content和content时设为False
                # 'thought_in_content': False,
                
                # 工具调用模板：推荐使用nous模板（适用于qwen3）
                'fncall_prompt_type': 'nous',
                
                # 最大输入长度，超过此长度的消息将被截断
                'max_input_tokens': self.max_input_tokens,
                
                # 模型API参数
                'temperature': self.temperature,
                'top_p': self.top_p,
                'top_k': self.top_k,  # top_k参数
                
                # 使用API的原生工具调用接口 (use_raw_api=True 会报错)
                'use_raw_api': False,
            }
        }
        # Agent 管理器（可注册/扩展不同的 Agent）
        self.agent_manager = AgentManager()
        


    def text_to_sql_agent(self) -> Any:
        """返回自然语言转SQL的 Agent 实例（通过 AgentManager 创建），保持 main 的调用方式不变。
        """
        llm_agent = get_chat_model(self.llm_cfg)
        assistant = Assistant(llm=llm_agent)

        agent = self.agent_manager.create('text_to_sql', llm_assistant=assistant)

        return agent

    def data_analysis_agent(self) -> Any:
        """返回数据分析 Agent 实例（通过 AgentManager 创建），保持 main 的调用方式不变。

        同时注入一个 LLM Assistant（如需更强的分析文本生成）。
        """
        llm_agent = get_chat_model(self.llm_cfg)
        assistant = Assistant(llm=llm_agent)

        agent = self.agent_manager.create('data_analysis', llm_assistant=assistant)

        return agent

    def sql_judge_agent(self) -> Any:
        """返回 SQL 判别 Agent 实例。"""
        llm_agent = get_chat_model(self.llm_cfg)
        assistant = Assistant(llm=llm_agent)
        agent = self.agent_manager.create('sql_judge', llm_assistant=assistant)
        return agent



def create_llm() -> LLM:
    load_dotenv()
    model_name = os.getenv("MODEL_NAME") or None
    model_server = os.getenv("MODEL_SERVER") or None
    api_key = os.getenv("API_KEY") or "EMPTY"
    enable_thinking = os.getenv("ENABLE_THINKING", "False").lower() == "true"
    max_input_tokens = int(os.getenv("MAX_INPUT_TOKENS", 32000))
    temperature = float(os.getenv("TEMPERATURE", 0.7))
    top_p = float(os.getenv("TOP_P", 0.8))

    llm_cfg = {
            'model': model_name,
            'model_server': model_server,
            'api_key': api_key,
            'generate_cfg': {
                # 针对vLLM/SGLang OAI API的思考模式配置
                'extra_body': {
                    'chat_template_kwargs': {'enable_thinking': enable_thinking}
                },
                # 当内容格式为 `<think>this is the thought</think>this is the answer` 时设为True
                # 当响应已被分离为reasoning_content和content时设为False
                # 'thought_in_content': False,
                
                # 工具调用模板：推荐使用nous模板（适用于qwen3）
                'fncall_prompt_type': 'nous',
                
                # 最大输入长度，超过此长度的消息将被截断
                'max_input_tokens': max_input_tokens,
                
                # 模型API参数
                'temperature': temperature,
                'top_p': top_p,
                'top_k': 20,  # top_k参数
                
                # 使用API的原生工具调用接口 (use_raw_api=True 会报错)
                'use_raw_api': False,
            }
        }
    return LLM(**llm_cfg)
