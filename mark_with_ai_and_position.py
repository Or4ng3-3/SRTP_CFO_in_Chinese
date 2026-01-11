#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
from dotenv import load_dotenv
import json
import time
import logging
import requests
import psycopg
import re
from psycopg.rows import dict_row
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()
# ---------------------------
# 配置
# ---------------------------
TABLE_NAME = "new_num_sentences"
NEW_TABLE = "number_level_analysis"  # 新表名：数词级别的分析

DB_CONFIG = {
    "host": os.getenv("PG_HOST"),
    "dbname": os.getenv("PG_DBNAME"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "port": int(os.getenv("PG_PORT", 5432))
}

BASE_URL = "https://api.vectorengine.ai"
API_KEY = os.getenv("VECTORENGINE_API_KEY")
GEMINI_MODEL = "gemini-2.5-flash"

REQUEST_TIMEOUT = 30
MODEL_RETRIES = 2

# 并发 & batch 参数
BATCH_SIZE = 20  # 增加batch size，因为现在是数词级别
MAX_WORKERS = 6

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# ---------------------------
# 数词提取函数 ⭐ 核心新增
# ---------------------------
def extract_chinese_numbers(text):
    """
    提取句子中所有的中文数词及其位置
    返回: [{'text': '三', 'start': 5, 'end': 6}, ...]
    """
    # 匹配连续的中文数字字符
    pattern = r'[零一二三四五六七八九十百千万亿]+'
    matches = []
    for match in re.finditer(pattern, text):
        matches.append({
            'text': match.group(),
            'start': match.start(),
            'end': match.end()
        })
    return matches

# ---------------------------
# 模型调用
# ---------------------------
def call_gemini_json(prompt: str, temperature: float = 0.1, retry: int = MODEL_RETRIES):
    url = f"{BASE_URL}/v1/chat/completions"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": GEMINI_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": temperature,
        "response_format": {"type": "json_object"}
    }

    last_err = None
    for i in range(retry):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            msg = data["choices"][0]["message"]["content"]
            if isinstance(msg, dict):
                return msg
            return json.loads(msg)
        except Exception as e:
            last_err = e
            logging.warning(f"模型请求失败（{i+1}/{retry}）：{e}")
            time.sleep(0.5 * (i + 1))
    raise last_err

# ---------------------------
# DB 初始化 ⭐ 修改表结构
# ---------------------------
def ensure_new_table(conn):
    """创建数词级别的分析表"""
    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {NEW_TABLE} (
            id BIGSERIAL PRIMARY KEY,
            source_id BIGINT NOT NULL,           -- 来源句子ID
            sentence TEXT NOT NULL,              -- 完整句子
            number_text TEXT NOT NULL,           -- 提取的数词文本
            number_start INT NOT NULL,           -- 数词起始位置
            number_end INT NOT NULL,             -- 数词结束位置
            number_type INTEGER DEFAULT NULL,    -- 0=基数/1=序数/2=固定短语/3=无数字含义
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """)
        # 创建索引加速查询
        cur.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{NEW_TABLE}_source 
        ON {NEW_TABLE}(source_id);
        """)
        cur.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_{NEW_TABLE}_type 
        ON {NEW_TABLE}(number_type);
        """)
        conn.commit()

def extract_and_insert_numbers(conn):
    """
    从 all_num_sentences 读取不含'第'的句子，
    提取每个数词，插入到新表
    """
    logging.info("开始提取数词并插入新表...")
    
    with conn.cursor(row_factory=dict_row) as cur:
        # 读取不含'第'的句子
        cur.execute(f"""
            SELECT id, content FROM {TABLE_NAME}
            WHERE content NOT LIKE '%第%'
        """)
        sentences = cur.fetchall()
        
        logging.info(f"找到 {len(sentences)} 条不含'第'的句子")
        
        insert_count = 0
        for row in sentences:
            sentence_id = row['id']
            sentence_text = row['content']
            
            # 提取所有数词
            numbers = extract_chinese_numbers(sentence_text)
            
            # 为每个数词插入一条记录
            for num_info in numbers:
                cur.execute(f"""
                    INSERT INTO {NEW_TABLE} 
                    (source_id, sentence, number_text, number_start, number_end)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    sentence_id,
                    sentence_text,
                    num_info['text'],
                    num_info['start'],
                    num_info['end']
                ))
                insert_count += 1
            
            if insert_count % 1000 == 0:
                conn.commit()
                logging.info(f"已插入 {insert_count} 条数词记录...")
        
        conn.commit()
        logging.info(f"✅ 数词提取完成，共插入 {insert_count} 条记录")

def fetch_rows_to_label(conn):
    """获取需要标注的数词记录"""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(f"""
            SELECT id, sentence, number_text, number_start, number_end
            FROM {NEW_TABLE}
            WHERE number_type IS NULL
            ORDER BY id
        """)
        rows = cur.fetchall()
    logging.info(f"查询到 {len(rows)} 条未标注的数词记录。")
    return rows

# ---------------------------
# Prompt 构造 ⭐ 重新设计
# ---------------------------
def build_batch_prompt(rows):
    """
    构造批量判断prompt
    rows: [{'id': 1, 'sentence': '三楼有一百人', 'number_text': '三', 'number_start': 0}, ...]
    """
    items = []
    for r in rows:
        items.append({
            "id": r["id"],
            "sentence": r["sentence"],
            "number": r["number_text"],
            "position": r["number_start"]
        })

    prompt = f"""你是一个中文数词语义分析专家。我会给你一个JSON数组，每个元素包含：
- id: 记录ID
- sentence: 完整的中文句子
- number: 句子中提取的数词文本
- position: 数词在句子中的起始位置

请判断每个数词在其句子中的语义类型，返回整数：
- 0: 基数含义（表示数量，如"一百人"的"一百"）
- 1: 序数含义（表示顺序，如"三楼"的"三"、"二月"的"二"）
- 2: 无数字含义（如"一向"的"一"、"万一"的"万", 这一类的表现形式多为短语，但并非所有短语中的数词都没有数字含义，比如说"同一"的一是一个的意思）

⚠️ 重要提示：
1. 即使数词看起来是基数词形式，也可能表达序数含义
2. 注意区分"三个苹果"（基数）和"三楼"（序数）
3. 每个数词独立判断，同一句子中不同数词可能有不同含义

请返回JSON格式：{{"results": [{{"id": 1, "type": 0}}, {{"id": 2, "type": 1}}, ...]}}

输入数据：
{json.dumps(items, ensure_ascii=False, indent=2)}
"""
    return prompt

# ---------------------------
# 子线程任务
# ---------------------------
def process_batch_api_task(batch_rows):
    """处理一个批次的API调用"""
    ids = [r["id"] for r in batch_rows]
    try:
        logging.info(f"🚀 处理批次: ids={ids[:5]}{'...' if len(ids) > 5 else ''} (共{len(ids)}条)")
        
        prompt = build_batch_prompt(batch_rows)
        result = call_gemini_json(prompt)
        
        # 打印部分返回结果用于调试
        result_preview = json.dumps(result, ensure_ascii=False)[:300]
        logging.info(f"📥 模型返回预览: {result_preview}...")
        
        return batch_rows, result
        
    except Exception as e:
        logging.error(f"❌ 批次处理失败 ids={ids[:5]}...: {e}")
        return batch_rows, None

# ---------------------------
# 主流程
# ---------------------------
def process_all():
    conn = None
    try:
        conn = psycopg.connect(**DB_CONFIG)

        # 1. 确保新表存在
        ensure_new_table(conn)
        
        # 2. 提取数词并插入（只在第一次运行时执行）
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) as cnt FROM {NEW_TABLE}")
            count = cur.fetchone()[0]
            
        if count == 0:
            extract_and_insert_numbers(conn)
        else:
            logging.info(f"表中已有 {count} 条记录，跳过数词提取步骤")

        # 3. 获取待标注的记录
        rows = fetch_rows_to_label(conn)
        if not rows:
            logging.info("✅ 无待处理任务，结束。")
            return

        # 4. 分批处理
        batches = [rows[i:i + BATCH_SIZE] for i in range(0, len(rows), BATCH_SIZE)]
        logging.info(
            f"🔧 并发配置：总记录={len(rows)}, 批次数={len(batches)}, "
            f"BATCH_SIZE={BATCH_SIZE}, MAX_WORKERS={MAX_WORKERS}"
        )

        with conn.cursor() as cur, ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = [pool.submit(process_batch_api_task, b) for b in batches]

            processed = 0
            failed = 0
            
            for fut in as_completed(futures):
                batch_rows, api_result = fut.result()

                # 处理失败情况
                if api_result is None:
                    for r in batch_rows:
                        cur.execute(
                            f"UPDATE {NEW_TABLE} SET number_type=-1 WHERE id=%s",
                            (r["id"],)
                        )
                        failed += 1
                    conn.commit()
                    logging.warning(f"⚠️ 批次失败，标记为-1")
                    continue

                # 解析结果
                results = api_result.get("results", [])
                if not isinstance(results, list):
                    logging.error(f"❌ 返回格式错误: {api_result}")
                    for r in batch_rows:
                        cur.execute(
                            f"UPDATE {NEW_TABLE} SET number_type=-1 WHERE id=%s",
                            (r["id"],)
                        )
                        failed += 1
                    conn.commit()
                    continue

                # 更新数据库
                id_map = {r["id"]: r for r in batch_rows}
                for item in results:
                    rid = item.get("id")
                    try:
                        num_type = int(item.get("type"))
                        if num_type not in (0, 1, 2):
                            num_type = -1
                    except Exception:
                        num_type = -1

                    if rid in id_map:
                        cur.execute(
                            f"UPDATE {NEW_TABLE} SET number_type=%s WHERE id=%s",
                            (num_type, rid)
                        )
                        processed += 1

                conn.commit()
                logging.info(
                    f"💾 批次完成: 成功{processed}/{len(rows)}, 失败{failed}"
                )

        logging.info(f"🎉 全部处理完成！成功: {processed}, 失败: {failed}")

        # 5. 输出统计信息
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(f"""
                SELECT 
                    number_type,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
                FROM {NEW_TABLE}
                WHERE number_type IS NOT NULL
                GROUP BY number_type
                ORDER BY number_type
            """)
            stats = cur.fetchall()
            
            logging.info("\n" + "="*50)
            logging.info("📊 数词类型统计:")
            logging.info("="*50)
            type_names = {
                -1: "处理失败",
                0: "基数含义",
                1: "序数含义",
                2: "固定短语"
            }
            for row in stats:
                type_name = type_names.get(row['number_type'], '未知')
                logging.info(
                    f"  {type_name:10s}: {row['count']:6d} 条 ({row['percentage']:5.2f}%)"
                )
            logging.info("="*50)

    except Exception:
        logging.exception("❌ 主流程异常")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            logging.info("数据库连接已关闭。")

# ---------------------------
if __name__ == "__main__":
    process_all()