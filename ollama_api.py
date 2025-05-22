import csv
import psycopg2
import ollama
from datetime import datetime
from sentence_transformers import SentenceTransformer

# PostgreSQL 연결 정보 (docker-compose.yml 기반)
DB_HOST = "127.0.0.1"
DB_PORT = 5433
DB_NAME = "news"
DB_USER = "ssafynews"
DB_PASSWORD = "ssafynews13"

# CSV 파일 경로
CSV_FILE_PATH = "./init_data.csv"  # 실제 CSV 파일 경로로 변경하세요

# CSV 컬럼 이름
CSV_NEWS_ID_COLUMN = "news_id"
CSV_TITLE_COLUMN = "title"
CSV_AUTHOR_COLUMN = "author"
CSV_LINK_COLUMN = "link"
CSV_FULL_TEXT_COLUMN = "full_text"
CSV_UPDATED_COLUMN = "updated" # CSV에 updated 컬럼이 있다고 가정

# DB 테이블 이름
TABLE_NAME = "news_api_newsarticle"
# DB 컬럼 이름
DB_ID_COLUMN = "news_id"
DB_TITLE_COLUMN = "title"
DB_AUTHOR_COLUMN = "author"
DB_LINK_COLUMN = "link"
DB_SUMMARY_COLUMN = "summary"
DB_UPDATED_COLUMN = "updated"
DB_FULL_TEXT_COLUMN = "full_text"
DB_CATEGORY_COLUMN = "category"
DB_KEYWORDS_COLUMN = "keywords"
DB_EMBEDDING_COLUMN = "embedding"

# 임베딩 모델 로드
EMBEDDING_MODEL_NAME = 'all-mpnet-base-v2'
embedding_model = SentenceTransformer(EMBEDDING_MODEL_NAME)

def summarize_text(text):
    """텍스트를 요약하고 사족 없이 바로 응답하는 함수"""
    prompt = f"""
    다음 기사의 주요 내용을 파악하여 어떤 내용인지 알 수 있도록 3문장으로 요약해주세요.
    답변은 사족을 붙이지 말고 바로 요약한 내용을 응답해주세요.
    ---
    {text}
    ---
    """
    try:
        response = ollama.chat(
            model='gemma3:4b-it-qat',
            messages=[{
                'role': 'user',
                'content': prompt
            }]
        )
        return response['message']['content'].strip()
    except Exception as e:
        print(f"Ollama 요약 중 오류 발생: {e}")
        return None

def classify_category_ollama(text, categories):
    """Ollama를 사용하여 텍스트를 주어진 카테고리 중 하나로 분류합니다."""
    prompt = f"""
    다음 텍스트를 가장 적절한 카테고리 하나로 분류해주세요: {', '.join(categories)}
    텍스트:
    ---
    {text}
    ---
    답변은 카테고리 이름만 정확하게 출력해주세요.
    """
    try:
        response = ollama.chat(
            model='gemma3:4b-it-qat',
            messages=[{
                'role': 'user',
                'content': prompt
            }]
        )
        output = response['message']['content'].strip()
        return output if output in categories else None
    except Exception as e:
        print(f"Ollama 카테고리 분류 중 오류 발생: {e}")
        return None

def extract_keywords_ollama(text, num_keywords=5):
    """Ollama를 사용하여 텍스트에서 핵심 키워드를 추출합니다."""
    prompt = f"""
    다음 텍스트에서 핵심 키워드 {num_keywords}개를 추출하여 쉼표로 구분하여 답변해주세요.
    ---
    {text}
    ---
    """
    try:
        response = ollama.chat(
            model='gemma3:4b-it-qat',
            messages=[{
                'role': 'user',
                'content': prompt
            }]
        )
        return [k.strip() for k in response['message']['content'].split(',')]
    except Exception as e:
        print(f"Ollama 키워드 추출 중 오류 발생: {e}")
        return None

def get_embedding(text):
    """텍스트에 대한 임베딩 벡터를 얻습니다."""
    try:
        return embedding_model.encode(text).tolist()
    except Exception as e:
        print(f"임베딩 생성 중 오류 발생: {e}")
        return None

def main():
    try:
        # PostgreSQL 연결
        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        cur = conn.cursor()

        with open(CSV_FILE_PATH, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            categories = ["IT_과학", "건강", "경제", "교육", "국제", "라이프스타일", "문화", "사건사고", "사회일반", "산업", "스포츠", "여성복지", "여행레저", "연예", "정치", "지역", "취미"]
            for row in reader:
                try:
                    news_id = row[CSV_NEWS_ID_COLUMN]
                    title = row[CSV_TITLE_COLUMN]
                    author = row[CSV_AUTHOR_COLUMN]
                    link = row[CSV_LINK_COLUMN]
                    full_text = row[CSV_FULL_TEXT_COLUMN]
                    updated_str = row.get(CSV_UPDATED_COLUMN)
                    updated = None
                    if updated_str:
                        try:
                            updated = datetime.fromisoformat(updated_str.replace('Z', '+00:00'))
                        except ValueError:
                            print(f"날짜 형식 변환 실패: {updated_str}. 기본값 None 사용.")

                    print(f"데이터 처리 시작 (ID: {news_id})...")
                    summary = summarize_text(full_text)
                    category = None
                    keywords = None
                    embedding = None

                    if summary:
                        category = classify_category_ollama(summary, categories)
                        keywords = extract_keywords_ollama(summary)
                        embedding = get_embedding(full_text) # 원문 기반 임베딩

                    if summary and category and keywords is not None and embedding is not None:
                        # 요약, 카테고리, 키워드, 임베딩 결과를 DB에 삽입
                        insert_query = f"""
                            INSERT INTO {TABLE_NAME} ({DB_ID_COLUMN}, {DB_TITLE_COLUMN}, {DB_AUTHOR_COLUMN}, {DB_LINK_COLUMN}, {DB_SUMMARY_COLUMN}, {DB_UPDATED_COLUMN}, {DB_FULL_TEXT_COLUMN}, {DB_CATEGORY_COLUMN}, {DB_KEYWORDS_COLUMN}, {DB_EMBEDDING_COLUMN})
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """
                        cur.execute(insert_query, (news_id, title, author, link, summary, updated, full_text, category, ','.join(keywords), embedding))
                        conn.commit()
                        print(f"ID {news_id} 처리 완료 및 DB 삽입 (카테고리: {category}, 키워드: {keywords}, 임베딩 완료).")
                    else:
                        print(f"ID {news_id} 요약, 카테고리 분류, 키워드 추출 또는 임베딩 생성 실패.")

                except KeyError as e:
                    print(f"CSV 파일에 필요한 컬럼 '{e}'가 없습니다: {e}")
                    conn.rollback()
                except psycopg2.IntegrityError as e:
                    conn.rollback()
                    print(f"PostgreSQL 무결성 오류 (중복 키 등): {e}")
                except Exception as e:
                    conn.rollback()
                    print(f"CSV 처리 중 오류 발생: {e}")

        print("모든 CSV 데이터 처리 완료.")

    except psycopg2.Error as e:
        print(f"PostgreSQL 오류: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()
            print("PostgreSQL 연결 종료.")

if __name__ == "__main__":
    main()

# import csv
# import psycopg2
# import ollama
# from datetime import datetime

# # PostgreSQL 연결 정보 (docker-compose.yml 기반)
# DB_HOST = "127.0.0.1"
# DB_PORT = 5433
# DB_NAME = "news"
# DB_USER = "ssafynews"
# DB_PASSWORD = "ssafynews13"

# # CSV 파일 경로
# CSV_FILE_PATH = "./init_data.csv"  # 실제 CSV 파일 경로로 변경하세요

# # CSV 컬럼 이름
# CSV_NEWS_ID_COLUMN = "news_id"
# CSV_TITLE_COLUMN = "title"
# CSV_AUTHOR_COLUMN = "author"
# CSV_LINK_COLUMN = "link"
# CSV_FULL_TEXT_COLUMN = "full_text"
# CSV_UPDATED_COLUMN = "updated" # CSV에 updated 컬럼이 있다고 가정

# # DB 테이블 이름
# TABLE_NAME = "news_api_newsarticle"
# # DB 컬럼 이름
# DB_ID_COLUMN = "news_id"
# DB_TITLE_COLUMN = "title"
# DB_AUTHOR_COLUMN = "author"
# DB_LINK_COLUMN = "link"
# DB_SUMMARY_COLUMN = "summary"
# DB_UPDATED_COLUMN = "updated"
# DB_FULL_TEXT_COLUMN = "full_text"
# DB_CATEGORY_COLUMN = "category"
# DB_KEYWORDS_COLUMN = "keywords"

# def summarize_text(text):
#     """텍스트를 요약하고 사족 없이 바로 응답하는 함수"""
#     prompt = f"""
#     다음 기사의 주요 내용을 파악하여 어떤 내용인지 알 수 있도록 3문장으로 요약해주세요.
#     답변은 사족을 붙이지 말고 바로 요약한 내용을 응답해주세요.
#     ---
#     {text}
#     ---
#     """
#     try:
#         response = ollama.chat(
#             model='gemma3:4b-it-qat',
#             messages=[{
#                 'role': 'user',
#                 'content': prompt
#             }]
#         )
#         return response['message']['content'].strip()
#     except Exception as e:
#         print(f"Ollama 요약 중 오류 발생: {e}")
#         return None

# def classify_category_ollama(text, categories):
#     """Ollama를 사용하여 텍스트를 주어진 카테고리 중 하나로 분류합니다."""
#     prompt = f"""
#     다음 텍스트를 가장 적절한 카테고리 하나로 분류해주세요: {', '.join(categories)}
#     텍스트:
#     ---
#     {text}
#     ---
#     답변은 카테고리 이름만 정확하게 출력해주세요.
#     """
#     try:
#         response = ollama.chat(
#             model='gemma3:4b-it-qat',
#             messages=[{
#                 'role': 'user',
#                 'content': prompt
#             }]
#         )
#         output = response['message']['content'].strip()
#         return output if output in categories else None
#     except Exception as e:
#         print(f"Ollama 카테고리 분류 중 오류 발생: {e}")
#         return None

# def extract_keywords_ollama(text, num_keywords=5):
#     """Ollama를 사용하여 텍스트에서 핵심 키워드를 추출합니다."""
#     prompt = f"""
#     다음 텍스트에서 핵심 키워드 {num_keywords}개를 추출하여 쉼표로 구분하여 답변해주세요.
#     ---
#     {text}
#     ---
#     """
#     try:
#         response = ollama.chat(
#             model='gemma3:4b-it-qat',
#             messages=[{
#                 'role': 'user',
#                 'content': prompt
#             }]
#         )
#         return [k.strip() for k in response['message']['content'].split(',')]
#     except Exception as e:
#         print(f"Ollama 키워드 추출 중 오류 발생: {e}")
#         return None

# def main():
#     try:
#         # PostgreSQL 연결
#         conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD)
#         cur = conn.cursor()

#         with open(CSV_FILE_PATH, 'r', encoding='utf-8') as csvfile:
#             reader = csv.DictReader(csvfile)
#             categories = ["IT_과학", "건강", "경제", "교육", "국제", "라이프스타일", "문화", "사건사고", "사회일반", "산업", "스포츠", "여성복지", "여행레저", "연예", "정치", "지역", "취미"]
#             for row in reader:
#                 try:
#                     news_id = row[CSV_NEWS_ID_COLUMN]
#                     title = row[CSV_TITLE_COLUMN]
#                     author = row[CSV_AUTHOR_COLUMN]
#                     link = row[CSV_LINK_COLUMN]
#                     full_text = row[CSV_FULL_TEXT_COLUMN]
#                     updated_str = row.get(CSV_UPDATED_COLUMN)
#                     updated = None
#                     if updated_str:
#                         try:
#                             updated = datetime.fromisoformat(updated_str.replace('Z', '+00:00'))
#                         except ValueError:
#                             print(f"날짜 형식 변환 실패: {updated_str}. 기본값 None 사용.")

#                     print(f"데이터 처리 시작 (ID: {news_id})...")
#                     summary = summarize_text(full_text)
#                     category = None
#                     keywords = None

#                     if summary:
#                         category = classify_category_ollama(summary, categories)
#                         keywords = extract_keywords_ollama(summary)

#                     if summary and category and keywords is not None:
#                         # 요약, 카테고리, 키워드 결과를 DB에 삽입 (embedding 제외)
#                         insert_query = f"""
#                             INSERT INTO {TABLE_NAME} ({DB_ID_COLUMN}, {DB_TITLE_COLUMN}, {DB_AUTHOR_COLUMN}, {DB_LINK_COLUMN}, {DB_SUMMARY_COLUMN}, {DB_UPDATED_COLUMN}, {DB_FULL_TEXT_COLUMN}, {DB_CATEGORY_COLUMN}, {DB_KEYWORDS_COLUMN})
#                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
#                             """
#                         cur.execute(insert_query, (news_id, title, author, link, summary, updated, full_text, category, ','.join(keywords)))
#                         conn.commit()
#                         print(f"ID {news_id} 처리 완료 및 DB 삽입 (카테고리: {category}, 키워드: {keywords}).")
#                     else:
#                         print(f"ID {news_id} 요약, 카테고리 분류 또는 키워드 추출 실패.")

#                 except KeyError as e:
#                     print(f"CSV 파일에 필요한 컬럼 '{e}'가 없습니다: {e}")
#                     conn.rollback()
#                 except psycopg2.IntegrityError as e:
#                     conn.rollback()
#                     print(f"PostgreSQL 무결성 오류 (중복 키 등): {e}")
#                 except Exception as e:
#                     conn.rollback()
#                     print(f"CSV 처리 중 오류 발생: {e}")

#         print("모든 CSV 데이터 처리 완료.")

#     except psycopg2.Error as e:
#         print(f"PostgreSQL 오류: {e}")
#     finally:
#         if conn:
#             cur.close()
#             conn.close()
#             print("PostgreSQL 연결 종료.")

# if __name__ == "__main__":
#     main()