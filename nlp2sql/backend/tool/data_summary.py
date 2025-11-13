from __future__ import annotations
from typing import Any
import pandas as pd
from pandas.api.types import is_numeric_dtype, is_datetime64_any_dtype


def data_summary(df: Any) -> str:
    """
    返回数据概览信息，包含字段、记录数与简单的取值范围/统计，并稳健地渲染预览。

    - 优先使用 DataFrame.to_markdown（若 tabulate 未安装则回退到 to_string）。
    - 仅展示前 10 行，避免输出过大。
    - 对数值列给出 min/max。
    - 非数值列仅在“字段”列表里展示名称，不再计算额外的统计，避免误导或高开销。
    """


    try:
        cols = list(getattr(df, "columns", []))
        n = int(getattr(df, "__len__", lambda: 0)())
    except Exception:
        cols = []
        n = 0

    lines = []
    lines.append(f"字段: {cols}")
    lines.append(f"记录数: {n}")

    # 仅对数值列计算简单统计（min/max/sum）
    try:
        stats = []
        for c in cols:
            try:
                s = df[c]
                if is_numeric_dtype(s):
                    # 更稳妥地计算数值 min/max（考虑到可能含有非数值/缺失）
                    s_num = pd.to_numeric(s, errors='coerce')
                    vmin = s_num.min()
                    vmax = s_num.max()
                    vsum = s_num.sum()
                    stats.append(f"- {c} (数值): min={vmin}, max={vmax}, sum={vsum}")
                # 非数值列不额外统计，保持简单明了
            except Exception:
                continue
        if stats:
            lines.append("字段范围/统计 (仅数值列，含求和):")
            lines.extend(stats[:20])  # 最多展示 20 条，避免过长
    except Exception:
        pass

    # 预览
    try:
        preview = df.head(10).to_markdown(index=False)  # 依赖 tabulate
    except Exception:
        try:
            preview = df.head(10).to_string(index=False)
        except Exception:
            preview = "<无法渲染预览>"
    lines.append("示例数据:")
    lines.append(preview)

    return "\n".join(lines)
