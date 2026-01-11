import os
os.environ["HF_ENDPOINT"] = "https://hf-mirror.com"
import re
from datasets import load_dataset
import psycopg
import sys
from dotenv import load_dotenv
# ----------------------------------------------------
# 1. 数据库配置
# ----------------------------------------------------
DB_CONFIG = {
    "host": os.getenv("PG_HOST"),
    "dbname": os.getenv("PG_DBNAME"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "port": int(os.getenv("PG_PORT", 5432))
}
TABLE_NAME = "new_num_sentences"

# ----------------------------------------------------
# 2. 其他配置
# ----------------------------------------------------
DATASET_ID = "opencsg/Fineweb-Edu-Chinese-V2.1"
MAX_DOCS_FOR_TEST = 10000 
COMMIT_INTERVAL = 50  # 每处理 50 个匹配句子提交一次事务
chinese_num_pattern = re.compile(r'[零一二三四五六七八九十百千万亿]')

# ----------------------------------------------------
# 3. 核心函数
# ----------------------------------------------------

def split_sentences(text):
    # 保持分句逻辑不变
    sentences = re.split(r'([。！？\n]+)', text)
    new_sents = []
    for i in range(0, len(sentences) - 1, 2):
        new_sents.append(sentences[i] + sentences[i+1])
    if len(sentences) % 2 != 0 and sentences[-1]:
        new_sents.append(sentences[-1])
    return [s.strip() for s in new_sents if s.strip()]

def setup_database(conn):
    """创建或确保目标表存在，并定义自增 ID 和内容字段。"""
    print(f"--- 正在设置 PostgreSQL 数据库表: {TABLE_NAME} ---")
    
    # 使用 cursor 提交 SQL 命令
    with conn.cursor() as cur:
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            content TEXT NOT NULL
        );
        """
        cur.execute(create_table_sql)
        conn.commit()
    print("--- 数据库表设置完成。 ---")


def process_dataset_and_save_to_sql():
    # 尝试建立数据库连接
    conn = None
    try:
        conn = psycopg.connect(**DB_CONFIG)
        setup_database(conn)
    except Exception as e:
        print(f"❌ 数据库连接失败或表创建失败: {e}")
        return

    print(f"正在连接 Hugging Face 加载数据集: {DATASET_ID} (流式模式)...")
    
    try:
        dataset = load_dataset(DATASET_ID, split="train", streaming=True)
    except Exception as e:
        print(f"加载失败，请检查网络或授权: {e}")
        conn.close()
        return

    print(f"--- 数据集对象已创建，准备开始迭代流... ---")
    print(f"开始处理... 匹配到的句子将保存到 PostgreSQL 表: {TABLE_NAME}")
    
    match_count = 0
    insert_sql = f"INSERT INTO {TABLE_NAME} (content) VALUES (%s);"

    try:
        with conn.cursor() as cur:
            for i, sample in enumerate(dataset):
                doc_id = sample.get('id', 'unknown')
                content = sample.get('text', '') 
                
                # 实时进度更新
                if i % 500 == 0 and i > 0:
                    sys.stdout.write(f"--- 进度更新: 已处理 {i} 篇文档。已保存 {match_count} 个匹配句子。 ---\r")
                    sys.stdout.flush()

                if not content:
                    continue

                # 分句
                sentences = split_sentences(content)
                
                # 简单正则筛选
                for sentence in sentences:
                    if chinese_num_pattern.search(sentence):
                        
                        clean_sent = sentence.strip()
                        
                        # 🌟 核心功能：插入数据库
                        cur.execute(insert_sql, (clean_sent,))
                        match_count += 1
                        
                        # 批量提交 (性能关键)
                        if match_count % COMMIT_INTERVAL == 0:
                            conn.commit()
                            
                            # 打印匹配结果 (避免被进度条覆盖)
                            print(f"\n[{doc_id}] 发现并已提交 (总数: {match_count}): {clean_sent}") 

                # 强制停止逻辑 (测试阶段)
                if i >= MAX_DOCS_FOR_TEST: 
                     break
        
        # 循环结束：提交所有剩余的事务
        conn.commit()
        print(f"\n--- 迭代循环结束 ---")

    except Exception as e:
        print(f"\n❌ 处理过程中发生错误，数据未完全保存。错误: {e}")
        # 发生错误时尝试回滚
        if conn:
            conn.rollback()

    finally:
        if conn:
            conn.close()
            print(f"数据库连接已关闭。")
            
    print(f"处理完成。共找到并保存 {match_count} 个包含中文数字的句子。")

if __name__ == "__main__":
    process_dataset_and_save_to_sql()