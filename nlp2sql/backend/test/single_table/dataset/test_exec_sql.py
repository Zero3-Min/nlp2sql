import os
import sys
import json
from pathlib import Path
from typing import Any, List
import argparse
import pymysql

def connect_mysql(host: str, port: int, user: str, password: str, db: str = None):
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=db if db else None,
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )

def _split_statements(sql_blob: str) -> List[str]:
    parts = [p.strip() for p in sql_blob.split(";")]
    return [p + ";" for p in parts if p]

def _collect_sqls(obj: Any) -> List[str]:
    found: List[str] = []
    def rec(x: Any):
        if isinstance(x, dict):
            for k, v in x.items():
                if isinstance(k, str) and "sql" in k.lower():
                    if isinstance(v, str):
                        found.append(v)
                    elif isinstance(v, list):
                        for it in v:
                            if isinstance(it, str):
                                found.append(it)
                rec(v)
        elif isinstance(x, list):
            for it in x:
                rec(it)
    rec(obj)
    return found

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="/home/minshunhua/data/P2/nlp2sql/backend/test/single_table/dataset", help="包含 jsonl 的目录")
    ap.add_argument("--show-success", action="store_true", help="也输出成功语句(默认只输出失败)")
    args = ap.parse_args()

    host = os.getenv("DB_HOST", "127.0.0.1")
    port = int(os.getenv("DB_PORT", "3306"))
    user = os.getenv("DB_USER", "root")
    password = os.getenv("DB_PASSWORD", "123456")
    database = os.getenv("DB_NAME", "testdb")

    print(f"[INFO] 启动, 工作目录参数: {args.dir}")
    target_dir = Path(args.dir).resolve()
    print(f"[INFO] 绝对路径: {target_dir}")

    if not target_dir.exists():
        print(f"[ERROR] 目录不存在: {target_dir}")
        sys.exit(2)

    files = sorted([p for p in target_dir.iterdir() if p.is_file() and p.suffix.lower() == ".jsonl"])
    print(f"[INFO] 找到 jsonl 文件数: {len(files)}")
    if not files:
        return

    try:
        conn = connect_mysql(host, port, user, password, database if database else None)
    except Exception as e:
        print(f"[FATAL] 数据库连接失败: {e}")
        sys.exit(3)

    cur = conn.cursor()

    total_files = 0
    total_sql = 0
    total_failed = 0

    for path in files:
        total_files += 1
        print(f"\n[FILE] 开始: {path.name}")
        file_sql = 0
        file_failed = 0
        try:
            with path.open("r", encoding="utf-8") as f:
                for lineno, line in enumerate(f, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception as e:
                        print(f"[ERROR][PARSE] {path.name}:line {lineno} => {e}")
                        continue
                    sql_blobs = _collect_sqls(obj)
                    if not sql_blobs:
                        continue
                    stmt_seq = 0
                    for blob in sql_blobs:
                        for stmt in _split_statements(blob):
                            stmt_seq += 1
                            file_sql += 1
                            total_sql += 1
                            if not stmt.strip():
                                continue
                            try:
                                cur.execute(stmt)
                                if not args.show_success:
                                    # 成功且不显示
                                    if not stmt.lower().startswith("select"):
                                        conn.commit()
                                else:
                                    # 显示成功
                                    if stmt.lower().startswith("select"):
                                        _ = cur.fetchall()
                                    else:
                                        conn.commit()
                                    print(f"[OK] {path.name}:line {lineno} stmt#{stmt_seq}")
                            except Exception as e:
                                file_failed += 1
                                total_failed += 1
                                print(f"[ERROR][EXEC] {path.name}:line {lineno} stmt#{stmt_seq} => {e}")
                                print(stmt)
                                try:
                                    conn.rollback()
                                except Exception:
                                    pass
        except Exception as e:
            print(f"[ERROR][FILE] 读取失败 {path.name}: {e}")
        print(f"[FILE] 结束: {path.name} 总语句 {file_sql} 失败 {file_failed}")

    try:
        cur.close()
    finally:
        conn.close()

    print(f"\n[SUMMARY] 文件数 {total_files} 语句总数 {total_sql} 失败总数 {total_failed}")

if __name__ == "__main__":
    main()