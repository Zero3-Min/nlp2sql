
"""
终端演示：列出数据库/表，使用 LLM 将自然语言转为 SQL 并执行
"""
from __future__ import annotations
import os
import sys
from pathlib import Path
import traceback
import pandas as pd
import numpy as np
from decimal import Decimal

# ensure backend package is importable
ROOT = Path(__file__).resolve().parents[1] 
sys.path.insert(0, str(ROOT))
# print(ROOT)
# try load settings module (optional)



import agent
from config.sql.sql import create_db  
    
from config.llm.llm import create_llm  


def choose_from_list(prompt: str, items: list[str]) -> str | None:
    if not items:
        print("没有可选项。")
        return None
    for i, it in enumerate(items, 1):
        print(f"{i}. {it}")
    while True:
        ans = input(f"{prompt} (输入序号或名称，q退出): ").strip()
        if ans.lower() == "q":
            return None
        if ans.isdigit():
            idx = int(ans) - 1
            if 0 <= idx < len(items):
                return items[idx]
        else:
            if ans in items:
                return ans
        print("无效输入，请重试。")


def rows_to_list(rows) -> list[str]:
    out = []
    if not rows:
        return out
    for r in rows:
        try:
            if isinstance(r, dict):
                vals = list(r.values())
                out.append(str(vals[0]) if vals else str(r))
            elif isinstance(r, (list, tuple)):
                out.append(str(r[0]) if r else str(r))
            else:
                out.append(str(r))
        except Exception:
            out.append(str(r))
    return out


def results_to_dataframe(results, columns=None) -> pd.DataFrame:
    """将查询结果转换为 DataFrame，尽量保留原始类型语义。

    优先使用 pandas 的 pyarrow 后端（需要 pandas>=2.0 且安装 pyarrow），
    以更好地支持可空整数、时间戳与十进制等类型；如果不可用，
    回退到 convert_dtypes()，避免把数值/布尔一概降级为 object。
    """
    def _coerce_decimal_to_float(df: pd.DataFrame) -> pd.DataFrame:
        """将 Decimal/decimal 列转换为 float64，避免被当作 object 影响后续分析绘图。"""
        try:
            # 1) 处理 pyarrow decimal 扩展类型
            for col in df.columns:
                dt_str = str(df[col].dtype).lower()
                if "decimal" in dt_str:
                    try:
                        # 优先转为 Arrow 浮点（若不支持则回退为 numpy float64）
                        df[col] = df[col].astype("float64[pyarrow]")
                    except Exception:
                        try:
                            df[col] = df[col].astype("float64")
                        except Exception:
                            pass

            # 2) 处理 object 列中含有 Decimal 的情况
            for col in df.columns:
                s = df[col]
                if s.dtype == object:
                    # 采样检查是否包含 Decimal
                    sample = s.dropna()
                    has_decimal = False
                    for v in sample.head(1000):
                        if isinstance(v, Decimal):
                            has_decimal = True
                            break
                    if has_decimal:
                        # 仅将 Decimal 转为 float，其他值保持不变；随后整体转为 numeric（无法转换的置 NaN）
                        s2 = s.apply(lambda v: float(v) if isinstance(v, Decimal) else (np.nan if v is None else v))
                        df[col] = pd.to_numeric(s2, errors='coerce')
        except Exception:
            # 任意异常直接返回原 df，避免影响主流程
            return df
        return df

    try:
        # 若已安装 pyarrow，使用 Arrow 扩展类型以更好保留类型
        import pyarrow  # noqa: F401
        df = pd.DataFrame(results, columns=columns, dtype_backend="pyarrow")
        df = _coerce_decimal_to_float(df)
        return df
    except Exception:
        df = pd.DataFrame(results, columns=columns)
        try:
            # 尽量推断为扩展类型（Int64/Boolean/String 等）
            df = df.convert_dtypes()
        except Exception:
            pass
        df = _coerce_decimal_to_float(df)
        return df


def main():
    db = create_db()
    if not db.connect_to_database():
        print("数据库连接失败，检查配置后重试。")
        return

    try:
        # === 1. 选择数据库 ===
        print("\n查询可用的数据库...")
        db_results = db.execute_query("SHOW DATABASES;")
        db_names = rows_to_list(db_results)
        chosen_db = choose_from_list("请选择数据库", db_names)
        if not chosen_db:
            print("已取消。")
            return

        if not db.select_database(chosen_db):
            print("无法切换到所选数据库，退出。")
            return

        # === 2. 选择表 ===
        print(f"\n列出数据库 `{chosen_db}` 的表...")
        tables_res = db.execute_query(f"SHOW TABLES FROM `{chosen_db}`;")
        table_names = rows_to_list(tables_res)
        chosen_table = choose_from_list("请选择表", table_names)
        if not chosen_table:
            print("已取消。")
            return

        # === 3. 创建 LLM 实例 ===
        llm = create_llm()

        user_nl = input("\n请输入你的查询（自然语言）： ").strip()
        if not user_nl:
            print("未输入查询。退出。")
            return

        # === 4. 自然语言转SQL ===
        nlp2sql = llm.text_to_sql_agent()
        generated_sql = nlp2sql.run(
            user_nl=user_nl, database=chosen_db, table=chosen_table, conn=db
        )
        print("\nLLM 生成的 SQL 建议：")
        print(generated_sql)
        accept = input("接受该 SQL 并执行？(y/n，n 则手动输入 SQL)： ").strip().lower()
        if accept != "y":
            manual = input("请输入要执行的 SQL（以 ; 结尾可选）： ").strip()
            if not manual:
                print("未输入 SQL，退出。")
                return
            sql_to_exec = manual
        else:
            sql_to_exec = generated_sql

        print("\n执行 SQL：", sql_to_exec)
        results = db.execute_query(sql_to_exec)

        # === 5. 检查查询结果 ===
        print("\n查询结果（前若干行）：")
        if not results:
            print("无结果或查询失败。")
            return
        else:
            if isinstance(results, list):
                for i, row in enumerate(results[:10], 1):
                    print(f"{i}. {row}")
            else:
                print(results)

        # === 6. 转换结果为DataFrame ===

        if isinstance(results, list) and isinstance(results[0], dict):
            results_df = results_to_dataframe(results)
        elif isinstance(results, list) and isinstance(results[0], (list, tuple)):
            # 若是元组列表（MySQL cursor）
            col_names = db.get_column_names(sql_to_exec)
            results_df = results_to_dataframe(results, columns=col_names)
        else:
            print("⚠️ 结果格式不支持自动分析。")
            return
        print(results_df)


        # === 7. 调用 data_analysis_agent 分析 ===
        print("\n开始调用智能数据分析 Agent ...\n")
        analysis_agent = llm.data_analysis_agent()
        analysis_table = analysis_agent.run(results_df, user_nl, mode='table')
        analysis_report = analysis_agent.run(results_df, user_nl, mode='report')

        print("\n=== 数据报表 ===\n")
        print(analysis_table)

        print("\n=== 数据报告 ===\n")
        print(analysis_report)

    except KeyboardInterrupt:
        print("\n中断。")
    except Exception:
        traceback.print_exc()
    finally:
        db.close()
        print("已关闭数据库连接。")




if __name__ == "__main__":
    main()
# /usr/bin/python3 /home/minshunhua/data/P1/vanna/src/nlp2sql/backend/app/main.py \ 
# DB_HOST=localhost DB_USER=root DB_PASSWORD=123456 DB_PORT=3306 \

