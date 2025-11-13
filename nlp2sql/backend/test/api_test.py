# import openai
 
# client = openai.OpenAI(
#     api_key="sk-WncTIwkfAtVQ0gRSSZDVmQ",  
#     base_url="https://llmapi.blsc.cn/v1/"
# )
 
# response = client.chat.completions.create(
#     model="DeepSeek-V3.1",  # model to send to the proxy
#     messages=[{"role": "user", "content": "Hello World"}],
# )
# print(response)

# import json, os
# p = "/home/minshunhua/data/P1/vanna/training_data/cybersyn-data-commons/questions.json"
# with open(p,'r') as f:
#     data = json.load(f)
# print(type(data), len(data))
# print(data[:3])  # 显示前3条，确认字段名


import os
from qwen_agent.agents import Assistant
from qwen_agent.llm import get_chat_model
from typing import List, Dict, Any


llm_cfg = {
        'model': 'qwen3-32B',
        'model_server': 'http://118.145.150.40:9033/v1',
        'api_key': 'key',
        'generate_cfg': {
            # 针对vLLM/SGLang OAI API的思考模式配置
            'extra_body': {
                'chat_template_kwargs': {'enable_thinking': 'false'}
            },
            # 当内容格式为 `<think>this is the thought</think>this is the answer` 时设为True
            # 当响应已被分离为reasoning_content和content时设为False
            # 'thought_in_content': False,
            
            # 工具调用模板：推荐使用nous模板（适用于qwen3）
            'fncall_prompt_type': 'nous',
            
            # 最大输入长度，超过此长度的消息将被截断
            'max_input_tokens': 48000,
            
            # 模型API参数
            'temperature': 0.8,
            'top_p': 0.7,
            'top_k': 20,  # top_k参数
            
            # 使用API的原生工具调用接口 (use_raw_api=True 会报错)
            'use_raw_api': False,
        }
    }
messages = [
    {"role": "user", "content": "请写一条SQL语句，查询vannadb库中Invoice表的所有列名和数据类型。"}
]
assistant = Assistant(llm=get_chat_model(llm_cfg))
for chunk in assistant.run(messages=messages):
    print(chunk, end='')
