
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg
import re
from psycopg.rows import dict_row
import os
from dotenv import load_dotenv

DB_CONFIG = {
    "host": os.getenv("PG_HOST"),
    "dbname": os.getenv("PG_DBNAME"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "port": int(os.getenv("PG_PORT", 5432))
}

TABLE_NAME = "new_num_sentences"

# 匹配“第 + 中文数词”
ORDINAL_PATTERN = re.compile(r'第[零一二三四五六七八九十百千万亿]+')

def main():
    conn = psycopg.connect(**DB_CONFIG)
    total_count = 0
    sentence_count = 0

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(f"""
            SELECT id, content
            FROM {TABLE_NAME}
            WHERE id <= 8764
              AND content LIKE '%第%'
            ORDER BY id
        """)
        rows = cur.fetchall()

    for row in rows:
        matches = ORDINAL_PATTERN.findall(row["content"])
        if matches:
            sentence_count += 1
            total_count += len(matches)

    conn.close()

    print("=" * 50)
    print("📊 含“第”的序数词统计结果")
    print("=" * 50)
    print(f"Source 范围           : 1 – 8764")
    print(f"含“第”的句子数       : {sentence_count}")
    print(f"“第 + 数词”总出现次数 : {total_count}")
    print("=" * 50)

if __name__ == "__main__":
    main()
