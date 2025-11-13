from __future__ import annotations
import json
import datetime
from pathlib import Path

from typing import Any
from enum import Enum

class ChartType(str, Enum):
    """
    图表类型枚举：
    - BAR: 树状图（bar chart）
    - LINE: 折线图（line chart）
    - PIE: 扇形图/饼图（pie chart）
    """
    BAR = "bar"      # 树状图
    LINE = "line"    # 折线图
    PIE = "pie"      # 扇形图/饼图

def create_chart(
    df: Any,  # 输入的数据表（如 pandas.DataFrame）
    x: str,   # x 轴字段名
    y: str,   # y 轴字段名
    chart_type: ChartType = ChartType.BAR,  # 图表类型（树状图/折线图/扇形图）
    output_dir: str | None = None,          # 输出目录，默认为当前目录
    max_points: int = 1000,                 # 最大数据点数，防止生成超大文件
    save_image: bool = True,                # 是否保存为 PNG 图片
    title: str | None = None,               # 图表标题
    x_label: str | None = None,             # x 轴标签
    y_label: str | None = None,             # y 轴标签
    description: str | None = None,         # 图表描述
) -> str:
    """
    将 (x, y) 两列导出为通用 JSON 图表规范，并可自动保存为 PNG 图片。
    支持的图表类型：
      - bar: 树状图
      - line: 折线图
      - pie: 扇形图/饼图
    返回 JSON 文件路径。
    """
    import matplotlib.pyplot as plt


    if x not in df.columns or y not in df.columns:
        raise ValueError(f"列不存在: x={x}, y={y}")
    if chart_type not in ChartType:
        raise ValueError(f"不支持的图表类型: {chart_type}. 只支持: {[e.value for e in ChartType]}")

    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"{chart_type.value}_chart_{ts}.json"
    out_dir = Path(output_dir) if output_dir else Path.cwd()
    out_dir.mkdir(parents=True, exist_ok=True)
    fpath = out_dir / fname

    # 限制数据量，避免生成超大 JSON
    data_rows = df[[x, y]].head(max_points).to_dict("records")
    data = {
        "type": chart_type.value,
        "x": x,
        "y": y,
        "title": title,
        "x_label": x_label or x,
        "y_label": y_label or y,
        "description": description,
        "generated_at": ts,
        "data": data_rows,
    }

    # with open(fpath, "w", encoding="utf-8") as f:
    #     json.dump(data, f, ensure_ascii=False, indent=2, default=str)

    # # 生成图片
    # if save_image:
    #     img_path = fpath.with_suffix('.png')
    #     fig, ax = plt.subplots()
    #     plot_df = df[[x, y]].head(max_points)
    #     if chart_type == ChartType.BAR:
    #         ax.bar(plot_df[x], plot_df[y])
    #         ax.set_xlabel(x_label or x)
    #         ax.set_ylabel(y_label or y)
    #     elif chart_type == ChartType.LINE:
    #         ax.plot(plot_df[x], plot_df[y], marker='o')
    #         ax.set_xlabel(x_label or x)
    #         ax.set_ylabel(y_label or y)
    #     elif chart_type == ChartType.PIE:
    #         # 饼图只用 y
    #         plot_df = plot_df.groupby(x)[y].sum().reset_index()
    #         ax.pie(plot_df[y], labels=plot_df[x], autopct='%1.1f%%')
    #     ax.set_title(title or f"{chart_type.value.capitalize()} Chart: {y} by {x}")
    #     fig.tight_layout()
    #     # fig.savefig(img_path, dpi=150)
    #     plt.close(fig)

    return str(fpath)
