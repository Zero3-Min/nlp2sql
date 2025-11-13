from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any


class BaseAgent(ABC):
	"""Agent 抽象基类，统一 run 接口。

	约定：
	- run 的最小契约为 run(*args, **kwargs) -> str
	- 具体 Agent 可声明更明确的签名（例如 run(df, user_nl)）。
	"""

	def __init__(self, **kwargs: Any) -> None:
		self.options = kwargs

	

	@abstractmethod
	def run(self, *args: Any, **kwargs: Any) -> str:  # pragma: no cover - interface
		"""执行 Agent 的主要逻辑并返回字符串结果。"""
		raise NotImplementedError

