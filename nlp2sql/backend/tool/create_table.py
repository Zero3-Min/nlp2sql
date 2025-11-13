from __future__ import annotations
import datetime
import uuid
from pathlib import Path
from typing import Any
import json

def create_table(
    df: Any,
    output_dir: str | None = None,
    max_rows: int = 1000,
    title: str = "Data Table",
    cache_id: str | None = None,
    # 新增：图片导出相关参数
    save_image: bool = True,
    image_max_rows: int = 30,
    cell_fontsize: int = 9,
    img_dpi: int = 150
) -> str:
    """
    将 DataFrame 导出为带元数据的 JSON（records 结构），支持表标题、唯一ID、可选 cache_id。
    同时（可选）生成一张二维表格的 PNG 图片保存到同目录。
    返回生成的 JSON 相对路径。
    """         
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_id = uuid.uuid4().hex[:8]
    fname = f"table_{unique_id}.json"
    out_dir = Path(output_dir) if output_dir else Path.cwd()
    out_dir.mkdir(parents=True, exist_ok=True)
    fpath = out_dir / fname

    df_display = df.head(max_rows)
    truncated = len(df) > max_rows
    table_data_output = {
        "title": title,
        "columns": df_display.columns.tolist(),
        "data": df_display.to_dict("records"),
        "metadata": {
            "total_rows": len(df),
            "displayed_rows": len(df_display),
            "total_columns": len(df.columns),
            "truncated": truncated,
            "created_at": ts,
            "data_source": f"cache:{cache_id}" if cache_id else None
        }
    }

    # with open(fpath, "w", encoding="utf-8") as f:
    #     json.dump(table_data_output, f, ensure_ascii=False, indent=2, default=str)

    # # 生成二维表格 PNG 图片
    # if save_image:
    #     try:
    #         import matplotlib.pyplot as plt
    #         df_img = df_display.head(max(1, min(image_max_rows, len(df_display))))
    #         # 动态尺寸：列越多越宽，行越多越高（设定上限避免过大）
    #         n_rows, n_cols = len(df_img), len(df_img.columns)
    #         width = min(22, 1.2 + 0.9 * max(6, n_cols))
    #         height = min(30, 1.2 + 0.45 * max(5, n_rows))

    #         fig, ax = plt.subplots(figsize=(width, height))
    #         ax.axis('off')
    #         ax.set_title(title, fontsize=cell_fontsize + 3, pad=12)

    #         table = ax.table(
    #             cellText=df_img.values,
    #             colLabels=df_img.columns.tolist(),
    #             loc='center',
    #             cellLoc='center'
    #         )
    #         table.auto_set_font_size(False)
    #         table.set_fontsize(cell_fontsize)
    #         # 调整表格缩放（行高稍加大，避免重叠）
    #         table.scale(1.0, 1.2)

    #         fig.tight_layout()
    #         img_path = fpath.with_suffix('.png')
    #         # fig.savefig(img_path, dpi=img_dpi, bbox_inches='tight')
    #         plt.close(fig)
    #     except Exception as e:
    #         # 避免因绘图失败影响主流程
    #         print(f"表格图片生成失败: {e}")

    return str(fpath)