# SSAFY News 데이터 CSV 내보내기 프로그램

PostgreSQL에서 SSAFY News 프로젝트의 모든 데이터를 CSV 파일로 내보내는 Python 프로그램입니다.
데모 데이터 생성 및 데이터 백업 목적으로 사용할 수 있습니다.

## 📋 기능

- **뉴스 기사 데이터**: 제목, 내용, 카테고리, 키워드 등
- **사용자 데이터**: 사용자명, 가입일 등 (개인정보 마스킹 처리)
- **상호작용 데이터**: 좋아요, 조회, 댓글 데이터
- **요약 보고서**: JSON 형태의 통계 리포트
- **데모 데이터셋**: 카테고리별 샘플 데이터

## 🛠️ 설치 및 설정

### 1. 필수 패키지 설치

```bash
pip install -r requirements.txt
```

### 2. 데이터베이스 설정 확인

`get_data_all.py` 파일에서 데이터베이스 연결 정보를 확인하세요:

```python
DB_CONFIG = {
    'host': 'localhost',
    'port': '5433',
    'database': 'news',
    'user': 'ssafynews',
    'password': 'ssafynews13'
}
```

### 3. PostgreSQL 서버 실행 확인

데이터베이스 서버가 실행 중인지 확인하세요.

## 🚀 실행 방법

```bash
python get_data_all.py
```

## 📁 출력 파일

프로그램 실행 후 `exported_data/` 폴더에 다음 파일들이 생성됩니다:

### CSV 파일
- `news_articles_YYYYMMDD_HHMMSS.csv` - 뉴스 기사 전체 데이터
- `users_YYYYMMDD_HHMMSS.csv` - 사용자 데이터 (개인정보 마스킹)
- `likes_YYYYMMDD_HHMMSS.csv` - 좋아요 데이터
- `views_YYYYMMDD_HHMMSS.csv` - 조회 데이터  
- `comments_YYYYMMDD_HHMMSS.csv` - 댓글 데이터
- `demo_news_sample_YYYYMMDD_HHMMSS.csv` - 데모용 샘플 데이터

### 보고서 파일
- `export_summary_YYYYMMDD_HHMMSS.json` - 내보내기 요약 통계

## 📊 데이터 구조

### 뉴스 기사 (news_articles)
```
news_id, title, author, link, summary, updated, full_text, category, keywords, has_embedding
```

### 사용자 (users)
```
user_id, username, email_masked, is_active, date_joined, last_login
```

### 좋아요 (likes)
```
like_id, user_id, news_id, created_at, news_title, category, username
```

### 조회 (views)
```
view_id, user_id, news_id, viewed_at, news_title, category, username
```

### 댓글 (comments)
```
comment_id, user_id, news_id, content, created_at, news_title, category, username
```

## 🔒 개인정보 보호

- 사용자 이메일은 자동으로 마스킹 처리됩니다 (예: abc*****@example.com)
- 비밀번호나 민감한 개인정보는 내보내지 않습니다

## 🎯 데모 데이터 활용

`demo_news_sample_*.csv` 파일은 다음과 같은 용도로 활용할 수 있습니다:

- 프레젠테이션용 샘플 데이터
- 개발/테스트 환경 초기 데이터
- 카테고리별 대표 기사 모음

## ⚠️ 주의사항

1. 데이터베이스 서버가 실행 중이어야 합니다
2. 대용량 데이터의 경우 실행 시간이 오래 걸릴 수 있습니다
3. 충분한 디스크 공간을 확보하세요
4. 네트워크 연결이 안정적이어야 합니다

## 🐛 문제 해결

### 연결 오류
```
❌ 데이터베이스 연결 실패: connection to server at "localhost" (::1), port 5433 failed
```
→ PostgreSQL 서버 실행 상태 및 포트 번호를 확인하세요

### 권한 오류
```
❌ 뉴스 기사 내보내기 실패: permission denied for table news_api_newsarticle
```
→ 데이터베이스 사용자 권한을 확인하세요

### 패키지 오류
```
ModuleNotFoundError: No module named 'psycopg2'
```
→ `pip install -r requirements.txt`로 필수 패키지를 설치하세요

## 📞 지원

문제가 발생하면 다음 정보와 함께 문의하세요:
- 오류 메시지 전문
- 실행 환경 (OS, Python 버전)
- 데이터베이스 버전 및 설정 

## ollama
```bash
ollama run gemma3:4b-it-qat
```

## Data Download
[click here](https://drive.google.com/file/d/1Ha55doyEl_-S_n7u-nDjTJX1TMW7aRhn/view?usp=drive_link)

