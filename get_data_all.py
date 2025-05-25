#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SSAFY News PostgreSQL 데이터 CSV 내보내기 프로그램
데모 데이터 생성용

사용법:
python get_data_all.py
"""

import psycopg2
import pandas as pd
import numpy as np
import os
import json
from datetime import datetime
import sys

# 데이터베이스 연결 설정
DB_CONFIG = {
    'host': 'localhost',
    'port': '5433',
    'database': 'news',
    'user': 'ssafynews',
    'password': 'ssafynews13'
}

# CSV 출력 디렉토리
OUTPUT_DIR = 'exported_data'

def create_output_directory():
    """출력 디렉토리 생성"""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"✅ 출력 디렉토리 생성: {OUTPUT_DIR}")

def connect_to_db():
    """PostgreSQL 데이터베이스 연결"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ PostgreSQL 연결 성공")
        return conn
    except psycopg2.Error as e:
        print(f"❌ 데이터베이스 연결 실패: {e}")
        return None

def export_news_articles(conn):
    """뉴스 기사 데이터 내보내기"""
    print("\n📰 뉴스 기사 데이터 내보내는 중...")
    
    query = """
    SELECT 
        news_id,
        title,
        author,
        link,
        summary,
        updated,
        full_text,
        category,
        keywords,
        CASE 
            WHEN embedding IS NOT NULL THEN 'TRUE'
            ELSE 'FALSE'
        END as has_embedding
    FROM news_api_newsarticle
    ORDER BY updated DESC;
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        
        # 날짜 포맷팅
        df['updated'] = pd.to_datetime(df['updated']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # 키워드 정리
        df['keywords'] = df['keywords'].fillna('')
        df['keywords'] = df['keywords'].str.replace('{', '').str.replace('}', '').str.replace('"', '')
        
        # CSV 저장
        filename = f"{OUTPUT_DIR}/news_articles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        print(f"✅ 뉴스 기사 {len(df)}개 내보내기 완료: {filename}")
        return df
        
    except Exception as e:
        print(f"❌ 뉴스 기사 내보내기 실패: {e}")
        return None

def export_user_data(conn):
    """사용자 데이터 내보내기 (개인정보 제외)"""
    print("\n👤 사용자 데이터 내보내는 중...")
    
    query = """
    SELECT 
        id as user_id,
        username,
        email,
        is_active,
        date_joined,
        last_login
    FROM accounts_user
    ORDER BY date_joined DESC;
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        
        # 이메일 마스킹 (개인정보 보호)
        df['email_masked'] = df['email'].apply(lambda x: x[:3] + '*' * (len(x.split('@')[0]) - 3) + '@' + x.split('@')[1] if '@' in str(x) else str(x))
        df = df.drop(['email'], axis=1)
        
        # 날짜 포맷팅
        df['date_joined'] = pd.to_datetime(df['date_joined']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df['last_login'] = pd.to_datetime(df['last_login']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # CSV 저장
        filename = f"{OUTPUT_DIR}/users_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        print(f"✅ 사용자 {len(df)}명 내보내기 완료: {filename}")
        return df
        
    except Exception as e:
        print(f"❌ 사용자 데이터 내보내기 실패: {e}")
        return None

def export_likes_data(conn):
    """좋아요 데이터 내보내기"""
    print("\n❤️ 좋아요 데이터 내보내는 중...")
    
    query = """
    SELECT 
        l.id as like_id,
        l.user_id,
        l.news_id,
        l.created_at,
        n.title as news_title,
        n.category,
        u.username
    FROM news_api_like l
    JOIN news_api_newsarticle n ON l.news_id = n.news_id
    JOIN accounts_user u ON l.user_id = u.id
    ORDER BY l.created_at DESC;
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        
        # 날짜 포맷팅
        df['created_at'] = pd.to_datetime(df['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # CSV 저장
        filename = f"{OUTPUT_DIR}/likes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        print(f"✅ 좋아요 {len(df)}개 내보내기 완료: {filename}")
        return df
        
    except Exception as e:
        print(f"❌ 좋아요 데이터 내보내기 실패: {e}")
        return None

def export_views_data(conn):
    """조회 데이터 내보내기"""
    print("\n👁️ 조회 데이터 내보내는 중...")
    
    query = """
    SELECT 
        v.id as view_id,
        v.user_id,
        v.news_id,
        v.viewed_at,
        n.title as news_title,
        n.category,
        u.username
    FROM news_api_view v
    JOIN news_api_newsarticle n ON v.news_id = n.news_id
    JOIN accounts_user u ON v.user_id = u.id
    ORDER BY v.viewed_at DESC;
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        
        # 날짜 포맷팅
        df['viewed_at'] = pd.to_datetime(df['viewed_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # CSV 저장
        filename = f"{OUTPUT_DIR}/views_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        print(f"✅ 조회 {len(df)}개 내보내기 완료: {filename}")
        return df
        
    except Exception as e:
        print(f"❌ 조회 데이터 내보내기 실패: {e}")
        return None

def export_comments_data(conn):
    """댓글 데이터 내보내기"""
    print("\n💬 댓글 데이터 내보내는 중...")
    
    query = """
    SELECT 
        c.id as comment_id,
        c.user_id,
        c.news_id,
        c.content,
        c.created_at,
        n.title as news_title,
        n.category,
        u.username
    FROM news_api_comment c
    JOIN news_api_newsarticle n ON c.news_id = n.news_id
    JOIN accounts_user u ON c.user_id = u.id
    ORDER BY c.created_at DESC;
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        
        # 날짜 포맷팅
        df['created_at'] = pd.to_datetime(df['created_at']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # CSV 저장
        filename = f"{OUTPUT_DIR}/comments_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        print(f"✅ 댓글 {len(df)}개 내보내기 완료: {filename}")
        return df
        
    except Exception as e:
        print(f"❌ 댓글 데이터 내보내기 실패: {e}")
        return None

def generate_summary_report(news_df, users_df, likes_df, views_df, comments_df):
    """요약 보고서 생성"""
    print("\n📊 요약 보고서 생성 중...")
    
    report = {
        'export_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_counts': {
            'news_articles': len(news_df) if news_df is not None else 0,
            'users': len(users_df) if users_df is not None else 0,
            'likes': len(likes_df) if likes_df is not None else 0,
            'views': len(views_df) if views_df is not None else 0,
            'comments': len(comments_df) if comments_df is not None else 0
        }
    }
    
    # 카테고리별 기사 수
    if news_df is not None:
        category_counts = news_df['category'].value_counts().to_dict()
        report['news_by_category'] = category_counts
    
    # 활성 사용자 수
    if users_df is not None:
        active_users = len(users_df[users_df['is_active'] == True])
        report['active_users'] = active_users
    
    # 보고서 저장
    filename = f"{OUTPUT_DIR}/export_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    print(f"✅ 요약 보고서 생성 완료: {filename}")
    print("\n📈 내보내기 요약:")
    print(f"  • 뉴스 기사: {report['total_counts']['news_articles']}개")
    print(f"  • 사용자: {report['total_counts']['users']}명")
    print(f"  • 좋아요: {report['total_counts']['likes']}개")
    print(f"  • 조회: {report['total_counts']['views']}개")
    print(f"  • 댓글: {report['total_counts']['comments']}개")

def create_demo_dataset(news_df):
    """데모용 샘플 데이터셋 생성"""
    if news_df is None or len(news_df) == 0:
        print("❌ 뉴스 데이터가 없어 데모 데이터셋을 생성할 수 없습니다.")
        return
    
    print("\n🎯 데모용 샘플 데이터셋 생성 중...")
    
    # 카테고리별 최신 기사 5개씩 선택
    demo_articles = []
    categories = news_df['category'].unique()
    
    for category in categories:
        if pd.notna(category):
            category_articles = news_df[news_df['category'] == category].head(5)
            demo_articles.append(category_articles)
    
    if demo_articles:
        demo_df = pd.concat(demo_articles, ignore_index=True)
        
        # 중복 제거
        demo_df = demo_df.drop_duplicates(subset=['news_id'])
        
        # 데모용 컬럼만 선택
        demo_columns = ['news_id', 'title', 'author', 'summary', 'updated', 'category', 'keywords']
        demo_df = demo_df[demo_columns]
        
        # CSV 저장
        filename = f"{OUTPUT_DIR}/demo_news_sample_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        demo_df.to_csv(filename, index=False, encoding='utf-8-sig')
        
        print(f"✅ 데모 데이터셋 {len(demo_df)}개 생성 완료: {filename}")

def main():
    """메인 실행 함수"""
    print("🚀 SSAFY News 데이터 CSV 내보내기 시작")
    print("=" * 50)
    
    # 출력 디렉토리 생성
    create_output_directory()
    
    # 데이터베이스 연결
    conn = connect_to_db()
    if not conn:
        print("❌ 프로그램을 종료합니다.")
        sys.exit(1)
    
    try:
        # 각 테이블별 데이터 내보내기
        news_df = export_news_articles(conn)
        users_df = export_user_data(conn)
        likes_df = export_likes_data(conn)
        views_df = export_views_data(conn)
        comments_df = export_comments_data(conn)
        
        # 요약 보고서 생성
        generate_summary_report(news_df, users_df, likes_df, views_df, comments_df)
        
        # 데모용 샘플 데이터셋 생성
        create_demo_dataset(news_df)
        
        print("\n🎉 모든 데이터 내보내기가 완료되었습니다!")
        print(f"📁 출력 폴더: {os.path.abspath(OUTPUT_DIR)}")
        
    except Exception as e:
        print(f"❌ 예기치 못한 오류 발생: {e}")
    
    finally:
        # 데이터베이스 연결 종료
        conn.close()
        print("✅ 데이터베이스 연결 종료")

if __name__ == "__main__":
    main()
