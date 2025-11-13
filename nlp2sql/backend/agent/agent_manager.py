from __future__ import annotations
from typing import Callable, Dict, Type, Any


class AgentManager:
	"""简单的 Agent 管理与工厂。

	用法：
		mgr = AgentManager()
		mgr.register('data_analysis', lambda **kw: DataAnalysisAgent(**kw))
		agent = mgr.create('data_analysis', foo='bar')
	"""

	def __init__(self) -> None:
		self._registry: Dict[str, Callable[..., Any]] = {}

	def register(self, name: str, factory: Callable[..., Any]) -> None:
		self._registry[name] = factory

	def create(self, name: str, **kwargs: Any) -> Any:
		if name not in self._registry:
			# 延迟加载，避免循环依赖
			if name == 'data_analysis':
				from .data_analysis_agent import DataAnalysisAgent
				self.register('data_analysis', lambda **kw: DataAnalysisAgent(**kw))
			elif name == 'text_to_sql':
				from .nlp2sql_agent import Nlp2SqlAgent
				self.register('text_to_sql', lambda **kw: Nlp2SqlAgent(**kw))
			elif name == 'sql_judge':
				from .sql_judge_agent import SqlJudgeAgent
				self.register('sql_judge', lambda **kw: SqlJudgeAgent(**kw))
			else:	
				raise KeyError(f"未注册的 Agent: {name}")
		return self._registry[name](**kwargs)

