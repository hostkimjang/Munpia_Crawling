from random import choice

from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, BigInteger, Text, DateTime, String, Boolean, and_, Integer, text
from sqlalchemy.orm import declarative_base, sessionmaker
import os
from datetime import datetime
import json
import time
import io
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import threading
import csv
import tempfile

load_dotenv()

BATCH_SIZE = 1000

# Load environment variables
USER = os.getenv('PG_USER')
PASSWORD = os.getenv('PG_PASSWORD')
HOST = os.getenv('PG_HOST')
PORT = os.getenv('PG_PORT')
DBNAME = os.getenv('PG_DB')

# Create a base class for declarative models
Base = declarative_base()


# Define Novelpia model
class Munpia(Base):
    __tablename__ = 'munpia'

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    platform = Column(Text)
    title = Column(Text)
    info = Column(Text)
    author = Column(Text)
    location = Column(Text)
    thumbnail = Column(Text)
    tags = Column(Text)
    chapter = Column(BigInteger)
    views = Column(BigInteger)
    newstatus = Column(Boolean)
    finishstatus = Column(Boolean)
    agegrade = Column(Boolean)
    registdate = Column(DateTime(timezone=True))
    updatedate = Column(DateTime(timezone=True))
    crawltime = Column(DateTime(timezone=True), server_default=text('CURRENT_TIMESTAMP'))

    def __repr__(self):
        return f"<munpia(id={self.id}, title='{self.title}', author='{self.author}')>"


db_url = f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}'
engine = create_engine(db_url, pool_pre_ping=True)
Session = sessionmaker(bind=engine)

def main_queries():
    with Session() as session:
        try:
            print("--- 상위 5개 소설 조회 ---")
            novels = session.query(Munpia).limit(5).all();
            [print(n) for n in novels]
            print("\n--- ID=1 소설 조회 ---")
            novel = session.query(Munpia).filter(Munpia.id == 1).first()
            if novel: print(f"Found: {novel.title} by {novel.author}")
            print("\n--- 마지막 5개 소설 조회 ---")
            last_novels = session.query(Munpia).order_by(Munpia.id.desc()).limit(5).all();
            [print(n) for n in last_novels]
        except Exception as e:
            print(f"쿼리 중 에러 발생: {e}")

def load_munpia_data(json_path='munpia_novel_info.json'):
    # json_path: Munpia 소설 리스트가 저장된 JSON 파일 경로
    with open(json_path, encoding='utf-8') as f:
        return json.load(f)


def chunked(iterable, size):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


def default_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)


def clean_text(val):
    if val is None:
        return ''
    import re
    cleaned = re.sub(r'<br\s*/?>', ' ', str(val), flags=re.IGNORECASE)
    cleaned = re.sub(r'<[^>]+>', '', cleaned)
    cleaned = cleaned.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
    cleaned = re.sub(r'\s+', ' ', cleaned)
    return cleaned.strip()


def store_db_munpia_pg_bulk_update(json_path='munpia_novel_info.json'):
    """임시 테이블을 사용한 bulk update 방식"""
    novel_list = load_munpia_data(json_path)
    start_time = time.time()
    dt = datetime.now()

    with Session() as session:
        # 1. 이미 DB에 존재하는 id 조회
        db_novels_dict = {n.id: n for n in session.query(Munpia)}
        db_ids = set(db_novels_dict.keys())
        print(f"DB에 {len(db_ids)}개 데이터 존재")

        insert_data, update_data, update_log = [], [], []

        # 2. 필드 매핑 (json key → db key)
        field_map = {
            "platform": "platform",
            "title": "title",
            "info": "info",
            "author": "author",
            "href": "location",
            "thumbnail": "thumbnail",
            "tag": "tags",
            "the_number_of_serials": "chapter",
            "view": "views",
            "newstatus": "newstatus",
            "finishstatus": "finishstatus",
            "agegrade": "agegrade",
            "registdate": "registdate",
            "updatedate": "updatedate"
        }

        for novel in novel_list:
            n_id = novel.get("id")
            if n_id is None: continue

            is_new = n_id not in db_ids
            db_novel = None if is_new else db_novels_dict[n_id]
            payload = {'id': n_id}
            changes = {}

            for json_key, orm_key in field_map.items():
                new_val = novel.get(json_key)
                old_val = None if is_new else getattr(db_novel, orm_key)

                # Boolean/Integer/Datetime 변환
                if orm_key in ["chapter", "views"]:
                    new_val = int(new_val or 0)
                if orm_key in ["newstatus", "finishstatus", "agegrade"]:
                    new_val = bool(new_val) if new_val is not None else False
                if orm_key in ["registdate", "updatedate"]:
                    if new_val and not isinstance(new_val, datetime):
                        try:
                            new_val = datetime.fromisoformat(new_val)
                        except Exception:
                            new_val = dt
                    
                    # 날짜 비교 시 시간대 제거하여 정확한 비교
                    if not is_new and old_val is not None and new_val is not None:
                        # 기존 값에서 날짜 부분만 추출 (시간대 제거)
                        old_date = old_val.replace(tzinfo=None).date()
                        new_date = new_val.replace(tzinfo=None).date()
                        if old_date == new_date:
                            continue  # 날짜가 같으면 변경사항으로 간주하지 않음

                if is_new or old_val != new_val:
                    payload[orm_key] = new_val
                    if not is_new:
                        # info 등 텍스트 필드는 clean_text로 정제해서 비교 및 로그 기록
                        if orm_key == "info":
                            before_clean = clean_text(old_val)
                            after_clean = clean_text(new_val)
                            if before_clean != after_clean:
                                changes[orm_key] = {
                                    "before": before_clean,
                                    "after": after_clean
                                }
                        else:
                            if str(old_val) != str(new_val):
                                changes[orm_key] = {"before": str(old_val), "after": str(new_val)}

            # 신규 row라면
            if is_new:
                payload.setdefault('crawltime', dt)
                insert_data.append(payload)
            elif changes:  # 변경사항이 하나라도 있으면만 update_data에 추가
                payload['crawltime'] = dt
                for col in Munpia.__table__.columns:
                    if col.key not in payload:
                        payload[col.key] = getattr(db_novel, col.key)
                if payload.get('author') is None:
                    payload['author'] = 'Unknown'
                if payload.get('title') is None:
                    payload['title'] = 'Unknown Title'
                if payload.get('platform') is None:
                    payload['platform'] = 'Munpia'
                update_data.append(payload)
                update_log.append({"ID": n_id, "Changes": changes})

        print(f"신규 {len(insert_data)}건, 업데이트 {len(update_data)}건")

        # 중복 제거
        unique_update_data = {}
        for row in update_data:
            unique_update_data[row['id']] = row
        update_data = list(unique_update_data.values())
        print(f"중복 제거 후 업데이트 {len(update_data)}건")

        # 3. Bulk Insert
        try:
            if insert_data:
                print(f"신규 데이터 삽입 시작... ({len(insert_data)}건)")
                for i, batch in enumerate(chunked(insert_data, BATCH_SIZE)):
                    session.bulk_insert_mappings(Munpia, batch)
                    print(f"  신규 데이터 배치 {i+1} 완료 ({len(batch)}건)")

            # 4. Bulk Update (임시테이블 + UPDATE JOIN)
            if update_data:
                print(f"임시 테이블을 사용한 bulk update 시작...")
                
                # 임시 테이블 생성
                temp_table_name = f"temp_munpia_update_{int(time.time())}"
                create_temp_table_sql = f"""
                    CREATE TEMP TABLE {temp_table_name} (
                        id BIGINT PRIMARY KEY,
                        platform TEXT,
                        title TEXT,
                        info TEXT,
                        author TEXT,
                        location TEXT,
                        thumbnail TEXT,
                        tags TEXT,
                        chapter BIGINT,
                        views BIGINT,
                        newstatus BOOLEAN,
                        finishstatus BOOLEAN,
                        agegrade BOOLEAN,
                        registdate TIMESTAMP WITH TIME ZONE,
                        updatedate TIMESTAMP WITH TIME ZONE,
                        crawltime TIMESTAMP WITH TIME ZONE
                    );
                """
                
                session.execute(text(create_temp_table_sql))
                print(f"임시 테이블 {temp_table_name} 생성 완료")
                
                # 임시 테이블에 데이터 삽입
                print(f"임시 테이블에 {len(update_data)}건 데이터 삽입 중...")
                
                # 컬럼 순서 정의
                columns = [
                    'id', 'platform', 'title', 'info', 'author', 'location', 'thumbnail',
                    'tags', 'chapter', 'views', 'newstatus', 'finishstatus', 'agegrade',
                    'registdate', 'updatedate', 'crawltime'
                ]
                
                # 배치 단위로 임시 테이블에 삽입
                for i, batch in enumerate(chunked(update_data, BATCH_SIZE)):
                    batch_values = []
                    for row in batch:
                        values = []
                        for col in columns:
                            val = row.get(col, None)
                            
                            # 숫자 컬럼
                            if col in ["chapter", "views"]:
                                try:
                                    values.append(int(val))
                                except (ValueError, TypeError):
                                    values.append(0)
                            # 불린 컬럼
                            elif col in ["newstatus", "finishstatus", "agegrade"]:
                                values.append(bool(val))
                            # 날짜 컬럼
                            elif col in ["registdate", "updatedate", "crawltime"]:
                                if isinstance(val, datetime):
                                    values.append(val)
                                elif val is None:
                                    values.append(dt)
                                else:
                                    try:
                                        values.append(datetime.fromisoformat(str(val)))
                                    except Exception:
                                        values.append(dt)
                            # 텍스트 컬럼
                            else:
                                if val is None or val == '':
                                    values.append(None)
                                else:
                                    cleaned = str(val)
                                    # HTML 태그 및 특수문자 제거
                                    import re
                                    cleaned = re.sub(r'<br\s*/?>', ' ', cleaned, flags=re.IGNORECASE)
                                    cleaned = re.sub(r'<[^>]+>', '', cleaned)  # 모든 HTML 태그 제거
                                    cleaned = cleaned.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
                                    cleaned = re.sub(r'\s+', ' ', cleaned)
                                    values.append(cleaned.strip())
                        batch_values.append(tuple(values))
                    
                    # 배치 삽입 - executemany 사용
                    insert_sql = f"INSERT INTO {temp_table_name} ({','.join(columns)}) VALUES ({','.join(['%s'] * len(columns))})"
                    
                    # executemany로 배치 삽입
                    with session.connection().connection.cursor() as cursor:
                        cursor.executemany(insert_sql, batch_values)
                    
                    print(f"  임시 테이블 배치 {i+1} 삽입 완료 ({len(batch)}건)")
                
                # UPDATE JOIN 실행
                print("UPDATE JOIN 실행 중...")
                update_join_sql = f"""
                    UPDATE munpia SET
                        platform = t.platform,
                        title = t.title,
                        info = t.info,
                        author = t.author,
                        location = t.location,
                        thumbnail = t.thumbnail,
                        tags = t.tags,
                        chapter = t.chapter,
                        views = t.views,
                        newstatus = t.newstatus,
                        finishstatus = t.finishstatus,
                        agegrade = t.agegrade,
                        registdate = t.registdate,
                        updatedate = t.updatedate,
                        crawltime = t.crawltime
                    FROM {temp_table_name} t
                    WHERE munpia.id = t.id;
                """
                
                result = session.execute(text(update_join_sql))
                updated_count = result.rowcount
                print(f"UPDATE JOIN 완료: {updated_count}건 업데이트")

            session.commit()
            print("DB 작업 커밋 완료")
        except Exception as e:
            print(f"[DB 작업 에러] 롤백합니다: {e}")
            session.rollback()
            raise

    end_time = time.time()
    print(f"총 {end_time - start_time:.2f}초 소요")

    # 로그 기록
    if update_log:
        log_directory = 'DB_Processing_Log'
        if not os.path.exists(log_directory): os.makedirs(log_directory)
        timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
        log_file_path = os.path.join(log_directory, f'{timestamp}-munpia-bulk-update-log.json')
        with open(log_file_path, 'w', encoding='utf-8') as f:
            json.dump(update_log, f, ensure_ascii=False, indent=4, default=default_serializer)
        print(f"변경로그 저장: {log_file_path}")


def store_db_munpia_pg(json_path='munpia_novel_info.json'):
    novel_list = load_munpia_data(json_path)
    start_time = time.time()
    dt = datetime.now()

    with Session() as session:
        # 1. 이미 DB에 존재하는 id 조회
        db_novels_dict = {n.id: n for n in session.query(Munpia)}
        db_ids = set(db_novels_dict.keys())
        print(f"DB에 {len(db_ids)}개 데이터 존재")

        insert_data, update_data, update_log = [], [], []

        # 2. 필드 매핑 (json key → db key)
        field_map = {
            "platform": "platform",
            "title": "title",
            "info": "info",
            "author": "author",
            "href": "location",
            "thumbnail": "thumbnail",
            "tag": "tags",
            "the_number_of_serials": "chapter",
            "view": "views",
            "newstatus": "newstatus",
            "finishstatus": "finishstatus",
            "agegrade": "agegrade",
            "registdate": "registdate",
            "updatedate": "updatedate"
        }

        for novel in novel_list:
            n_id = novel.get("id")
            if n_id is None: continue

            is_new = n_id not in db_ids
            db_novel = None if is_new else db_novels_dict[n_id]
            payload = {'id': n_id}
            changes = {}

            for json_key, orm_key in field_map.items():
                new_val = novel.get(json_key)
                old_val = None if is_new else getattr(db_novel, orm_key)

                # Boolean/Integer/Datetime 변환
                if orm_key in ["chapter", "views"]:
                    new_val = int(new_val or 0)
                if orm_key in ["newstatus", "finishstatus", "agegrade"]:
                    new_val = bool(new_val) if new_val is not None else False
                if orm_key in ["registdate", "updatedate"]:
                    if new_val and not isinstance(new_val, datetime):
                        try:
                            new_val = datetime.fromisoformat(new_val)
                        except Exception:
                            new_val = dt
                    
                    # 날짜 비교 시 시간대 제거하여 정확한 비교
                    if not is_new and old_val is not None and new_val is not None:
                        # 기존 값에서 날짜 부분만 추출 (시간대 제거)
                        old_date = old_val.replace(tzinfo=None).date()
                        new_date = new_val.replace(tzinfo=None).date()
                        if old_date == new_date:
                            continue  # 날짜가 같으면 변경사항으로 간주하지 않음

                if is_new or old_val != new_val:
                    payload[orm_key] = new_val
                    if not is_new:
                        # info 등 텍스트 필드는 clean_text로 정제해서 비교 및 로그 기록
                        if orm_key == "info":
                            before_clean = clean_text(old_val)
                            after_clean = clean_text(new_val)
                            if before_clean != after_clean:
                                changes[orm_key] = {
                                    "before": before_clean,
                                    "after": after_clean
                                }
                        else:
                            if str(old_val) != str(new_val):
                                changes[orm_key] = {"before": str(old_val), "after": str(new_val)}

            # 신규 row라면
            if is_new:
                payload.setdefault('crawltime', dt)
                insert_data.append(payload)
            elif changes:  # 변경사항이 하나라도 있으면만 update_data에 추가
                payload['crawltime'] = dt
                for col in Munpia.__table__.columns:
                    if col.key not in payload:
                        payload[col.key] = getattr(db_novel, col.key)
                if payload.get('author') is None:
                    payload['author'] = 'Unknown'
                if payload.get('title') is None:
                    payload['title'] = 'Unknown Title'
                if payload.get('platform') is None:
                    payload['platform'] = 'Munpia'
                update_data.append(payload)
                update_log.append({"ID": n_id, "Changes": changes})

        print(f"신규 {len(insert_data)}건, 업데이트 {len(update_data)}건")

        # 중복 제거
        unique_update_data = {}
        for row in update_data:
            unique_update_data[row['id']] = row
        update_data = list(unique_update_data.values())
        print(f"중복 제거 후 업데이트 {len(update_data)}건")

        # 디버깅: author 필드가 누락된 데이터 확인
        missing_author_data = [row for row in update_data if row.get('author') is None]
        if missing_author_data:
            print(f"경고: author 필드가 누락된 데이터 {len(missing_author_data)}건 발견")
            for i, row in enumerate(missing_author_data[:5]):  # 처음 5개만 출력
                print(f"  {i+1}. ID: {row.get('id')}, title: {row.get('title')}, author: {row.get('author')}")

        # 3. Bulk Insert
        try:
            if insert_data:
                print(f"신규 데이터 삽입 시작... ({len(insert_data)}건)")
                for i, batch in enumerate(chunked(insert_data, BATCH_SIZE)):
                    session.bulk_insert_mappings(Munpia, batch)
                    print(f"  신규 데이터 배치 {i+1} 완료 ({len(batch)}건)")
            # 4. Bulk Update (임시테이블 + UPDATE JOIN)
            if update_data:
                print(f"직접 UPDATE 문으로 데이터 업데이트 중...")
                
                # 컬럼 순서 정의
                columns = [
                    'id', 'platform', 'title', 'info', 'author', 'location', 'thumbnail',
                    'tags', 'chapter', 'views', 'newstatus', 'finishstatus', 'agegrade',
                    'registdate', 'updatedate', 'crawltime'
                ]
                
                print(f"총 {len(update_data)}건의 데이터를 처리합니다...")
                
                def update_batch(batch_data, pbar, pbar_lock):
                    """배치 데이터를 업데이트하는 함수"""
                    local_engine = create_engine(db_url, pool_pre_ping=True)
                    local_session = sessionmaker(bind=local_engine)()
                    updated_count = 0
                    
                    try:
                        for row in batch_data:
                            # UPDATE 문 생성
                            update_sql = f"""
                                UPDATE munpia SET
                                    platform = %s, title = %s, info = %s, author = %s,
                                    location = %s, thumbnail = %s, tags = %s, chapter = %s, views = %s,
                                    newstatus = %s, finishstatus = %s, agegrade = %s,
                                    registdate = %s, updatedate = %s, crawltime = %s
                                WHERE id = %s;
                            """
                            
                            # 값 준비
                            values = []
                            for col in columns:
                                if col == 'id':
                                    continue  # WHERE 절에서 사용
                                val = row.get(col, None)
                                
                                # 숫자 컬럼
                                if col in ["chapter", "views"]:
                                    try:
                                        values.append(int(val))
                                    except (ValueError, TypeError):
                                        values.append(0)
                                # 불린 컬럼
                                elif col in ["newstatus", "finishstatus", "agegrade"]:
                                    values.append(bool(val))
                                # 날짜 컬럼
                                elif col in ["registdate", "updatedate", "crawltime"]:
                                    if isinstance(val, datetime):
                                        values.append(val)
                                    elif val is None:
                                        values.append(dt)
                                    else:
                                        try:
                                            values.append(datetime.fromisoformat(str(val)))
                                        except Exception:
                                            values.append(dt)
                                # 텍스트 컬럼
                                else:
                                    if val is None or val == '':
                                        values.append(None)
                                    else:
                                        cleaned = str(val)
                                        cleaned = cleaned.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
                                        cleaned = cleaned.replace('\v', ' ').replace('\f', ' ')
                                        # HTML 태그 및 특수문자 제거
                                        import re
                                        cleaned = re.sub(r'<br\s*/?>', ' ', cleaned, flags=re.IGNORECASE)
                                        cleaned = re.sub(r'<[^>]+>', '', cleaned)  # 모든 HTML 태그 제거
                                        cleaned = cleaned.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
                                        cleaned = re.sub(r'\s+', ' ', cleaned)
                                        values.append(cleaned.strip())
                            
                            # ID 추가 (WHERE 절용)
                            values.append(row['id'])
                            
                            # UPDATE 실행
                            with local_session.connection().connection.cursor() as cursor:
                                cursor.execute(update_sql, values)
                                if cursor.rowcount > 0:
                                    updated_count += 1
                            
                            # 스레드 안전하게 진행률 업데이트
                            with pbar_lock:
                                pbar.update(1)
                        
                        local_session.commit()
                        return updated_count
                    except Exception as e:
                        local_session.rollback()
                        print(f"배치 업데이트 중 오류: {e}")
                        return 0
                    finally:
                        local_session.close()
                        local_engine.dispose()
                
                # 병렬 처리를 위한 배치 분할
                batch_size = 500  # 더 작은 배치 크기
                batches = []
                for i in range(0, len(update_data), batch_size):
                    batches.append(update_data[i:i + batch_size])
                
                print(f"총 {len(batches)}개 배치를 병렬로 처리합니다...")
                
                # 병렬 처리 실행
                total_updated = 0
                max_workers = min(8, len(batches))  # 최대 8개 스레드
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # tqdm으로 진행률 표시 (데이터 개수 기준)
                    with tqdm(total=len(update_data), desc="데이터 처리", unit="건") as pbar:
                        pbar_lock = threading.Lock()
                        # 모든 배치를 제출
                        future_to_batch = {executor.submit(update_batch, batch, pbar, pbar_lock): i for i, batch in enumerate(batches)}
                        
                        # 완료된 작업들을 처리
                        for future in as_completed(future_to_batch):
                            batch_num = future_to_batch[future]
                            try:
                                updated_count = future.result()
                                total_updated += updated_count
                                pbar.set_postfix({
                                    '완료': f"{batch_num + 1}/{len(batches)}",
                                    '업데이트': updated_count,
                                    '총 업데이트': total_updated
                                })
                            except Exception as e:
                                print(f"  배치 {batch_num + 1} 처리 중 오류: {e}")
                
                print(f"총 {total_updated}건 업데이트 완료")

            session.commit()
            print("DB 작업 커밋 완료")
        except Exception as e:
            print(f"[DB 작업 에러] 롤백합니다: {e}")
            session.rollback()
            raise

    end_time = time.time()
    print(f"총 {end_time - start_time:.2f}초 소요")

    # 로그 기록
    if update_log:
        log_directory = 'DB_Processing_Log'
        if not os.path.exists(log_directory): os.makedirs(log_directory)
        timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
        log_file_path = os.path.join(log_directory, f'{timestamp}-munpia-update-log.json')
        with open(log_file_path, 'w', encoding='utf-8') as f:
            json.dump(update_log, f, ensure_ascii=False, indent=4, default=default_serializer)
        print(f"변경로그 저장: {log_file_path}")


def store_db_munpia_pg_ctas(json_path='munpia_novel_info.json'):
    """CTAS (Create Table As Select) 방식을 사용한 업데이트"""
    novel_list = load_munpia_data(json_path)
    start_time = time.time()
    dt = datetime.now()

    with Session() as session:
        # 1. 이미 DB에 존재하는 id 조회
        db_novels_dict = {n.id: n for n in session.query(Munpia)}
        db_ids = set(db_novels_dict.keys())
        print(f"DB에 {len(db_ids)}개 데이터 존재")

        insert_data, update_data, update_log = [], [], []

        # 2. 필드 매핑 (json key → db key)
        field_map = {
            "platform": "platform",
            "title": "title",
            "info": "info",
            "author": "author",
            "href": "location",
            "thumbnail": "thumbnail",
            "tag": "tags",
            "the_number_of_serials": "chapter",
            "view": "views",
            "newstatus": "newstatus",
            "finishstatus": "finishstatus",
            "agegrade": "agegrade",
            "registdate": "registdate",
            "updatedate": "updatedate"
        }

        for novel in novel_list:
            n_id = novel.get("id")
            if n_id is None: continue

            is_new = n_id not in db_ids
            db_novel = None if is_new else db_novels_dict[n_id]
            payload = {'id': n_id}
            changes = {}

            for json_key, orm_key in field_map.items():
                new_val = novel.get(json_key)
                old_val = None if is_new else getattr(db_novel, orm_key)

                # Boolean/Integer/Datetime 변환
                if orm_key in ["chapter", "views"]:
                    new_val = int(new_val or 0)
                if orm_key in ["newstatus", "finishstatus", "agegrade"]:
                    new_val = bool(new_val) if new_val is not None else False
                if orm_key in ["registdate", "updatedate"]:
                    if new_val and not isinstance(new_val, datetime):
                        try:
                            new_val = datetime.fromisoformat(new_val)
                        except Exception:
                            new_val = dt
                    
                    # 날짜 비교 시 시간대 제거하여 정확한 비교
                    if not is_new and old_val is not None and new_val is not None:
                        # 기존 값에서 날짜 부분만 추출 (시간대 제거)
                        old_date = old_val.replace(tzinfo=None).date()
                        new_date = new_val.replace(tzinfo=None).date()
                        if old_date == new_date:
                            continue  # 날짜가 같으면 변경사항으로 간주하지 않음

                if is_new or old_val != new_val:
                    payload[orm_key] = new_val
                    if not is_new:
                        # info 등 텍스트 필드는 clean_text로 정제해서 비교 및 로그 기록
                        if orm_key == "info":
                            before_clean = clean_text(old_val)
                            after_clean = clean_text(new_val)
                            if before_clean != after_clean:
                                changes[orm_key] = {
                                    "before": before_clean,
                                    "after": after_clean
                                }
                        else:
                            if str(old_val) != str(new_val):
                                changes[orm_key] = {"before": str(old_val), "after": str(new_val)}

            # 신규 row라면
            if is_new:
                payload.setdefault('crawltime', dt)
                insert_data.append(payload)
            elif changes:
                # UPDATE 대상만
                payload['crawltime'] = dt
                # 빠진 컬럼 채우기
                for col in Munpia.__table__.columns:
                    if col.key not in payload:
                        payload[col.key] = getattr(db_novel, col.key)
                
                # 필수 필드에 대한 기본값 설정
                if payload.get('author') is None:
                    payload['author'] = 'Unknown'
                if payload.get('title') is None:
                    payload['title'] = 'Unknown Title'
                if payload.get('platform') is None:
                    payload['platform'] = 'Munpia'
                
                update_data.append(payload)
                update_log.append({"ID": n_id, "Changes": changes})

        print(f"신규 {len(insert_data)}건, 업데이트 {len(update_data)}건")

        # 중복 제거
        unique_update_data = {}
        for row in update_data:
            unique_update_data[row['id']] = row
        update_data = list(unique_update_data.values())
        print(f"중복 제거 후 업데이트 {len(update_data)}건")

        # 3. Bulk Insert
        try:
            if insert_data:
                print(f"신규 데이터 삽입 시작... ({len(insert_data)}건)")
                for i, batch in enumerate(chunked(insert_data, BATCH_SIZE)):
                    session.bulk_insert_mappings(Munpia, batch)
                    print(f"  신규 데이터 배치 {i+1} 완료 ({len(batch)}건)")

            # 4. CTAS 방식으로 업데이트
            if update_data:
                print(f"CTAS 방식으로 업데이트 시작...")
                
                # 업데이트할 ID 목록
                update_ids = [row['id'] for row in update_data]
                update_ids_str = ','.join(map(str, update_ids))
                
                # CTAS 쿼리 생성
                temp_table_name = f"temp_munpia_ctas_{int(time.time())}"
                
                # 1단계: 기존 데이터를 복사하여 임시 테이블 생성
                print("1단계: 기존 데이터로 임시 테이블 생성 중...")
                ctas_sql = f"""
                    CREATE TABLE {temp_table_name} AS 
                    SELECT * FROM munpia 
                    WHERE id NOT IN ({update_ids_str});
                """
                session.execute(text(ctas_sql))
                
                # 컬럼 순서 정의
                columns = [
                    'id', 'platform', 'title', 'info', 'author', 'location', 'thumbnail',
                    'tags', 'chapter', 'views', 'newstatus', 'finishstatus', 'agegrade',
                    'registdate', 'updatedate', 'crawltime'
                ]
                
                # 2단계: 업데이트할 데이터를 임시 테이블에 삽입
                print("2단계: 업데이트 데이터를 임시 테이블에 삽입 중...")
                
                # 배치 단위로 삽입 (VALUES 절 길이 제한 해결)
                batch_size = 1000  # 한 번에 처리할 데이터 수
                for i, batch in enumerate(chunked(update_data, batch_size)):
                    if not batch:  # 빈 배치 건너뛰기
                        continue
                        
                    # SQLAlchemy 파라미터 바인딩 방식으로 변경
                    batch_values = []
                    for row in batch:
                        values = []
                        for col in columns:
                            val = row.get(col, None)
                            
                            # 숫자 컬럼
                            if col in ["chapter", "views"]:
                                try:
                                    values.append(int(val))
                                except (ValueError, TypeError):
                                    values.append(0)
                            # 불린 컬럼
                            elif col in ["newstatus", "finishstatus", "agegrade"]:
                                values.append(bool(val))
                            # 날짜 컬럼
                            elif col in ["registdate", "updatedate", "crawltime"]:
                                if isinstance(val, datetime):
                                    values.append(val)
                                elif val is None:
                                    values.append(dt)
                                else:
                                    try:
                                        values.append(datetime.fromisoformat(str(val)))
                                    except Exception:
                                        values.append(dt)
                            # 텍스트 컬럼
                            else:
                                if val is None or val == '':
                                    values.append(None)
                                else:
                                    cleaned = str(val)
                                    # HTML 태그 및 특수문자 제거
                                    import re
                                    cleaned = re.sub(r'<br\s*/?>', ' ', cleaned, flags=re.IGNORECASE)
                                    cleaned = re.sub(r'<[^>]+>', '', cleaned)  # 모든 HTML 태그 제거
                                    cleaned = cleaned.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
                                    cleaned = re.sub(r'\s+', ' ', cleaned)
                                    values.append(cleaned.strip())
                        
                        batch_values.append(tuple(values))
                    
                    # executemany로 배치 삽입
                    insert_sql = f"INSERT INTO {temp_table_name} ({','.join(columns)}) VALUES ({','.join(['%s'] * len(columns))})"
                    
                    with session.connection().connection.cursor() as cursor:
                        cursor.executemany(insert_sql, batch_values)
                    
                    print(f"  배치 {i+1} 삽입 완료 ({len(batch)}건)")
                
                # 3단계: 원본 테이블을 백업하고 임시 테이블을 교체
                print("3단계: 테이블 교체 중...")
                backup_table_name = f"munpia_backup_{int(time.time())}"
                
                # 백업 생성
                backup_sql = f"ALTER TABLE munpia RENAME TO {backup_table_name};"
                session.execute(text(backup_sql))
                
                # 임시 테이블을 원본 테이블로 변경
                rename_sql = f"ALTER TABLE {temp_table_name} RENAME TO munpia;"
                session.execute(text(rename_sql))
                
                # 인덱스 및 제약조건 재생성 (필요한 경우)
                print("4단계: 인덱스 및 제약조건 재생성 중...")
                
                # Primary Key 재생성
                pk_sql = "ALTER TABLE munpia ADD CONSTRAINT munpia_pkey PRIMARY KEY (id);"
                session.execute(text(pk_sql))
                
                # Auto increment 설정 (PostgreSQL의 경우)
                sequence_sql = """
                    CREATE SEQUENCE IF NOT EXISTS munpia_id_seq;
                    ALTER TABLE munpia ALTER COLUMN id SET DEFAULT nextval('munpia_id_seq');
                    ALTER SEQUENCE munpia_id_seq OWNED BY munpia.id;
                    SELECT setval('munpia_id_seq', (SELECT MAX(id) FROM munpia));
                """
                session.execute(text(sequence_sql))
                
                print(f"CTAS 업데이트 완료: {len(update_data)}건 업데이트")
                print(f"백업 테이블: {backup_table_name}")

            session.commit()
            print("DB 작업 커밋 완료")
        except Exception as e:
            print(f"[DB 작업 에러] 롤백합니다: {e}")
            session.rollback()
            raise

    end_time = time.time()
    print(f"총 {end_time - start_time:.2f}초 소요")

    # 로그 기록
    if update_log:
        log_directory = 'DB_Processing_Log'
        if not os.path.exists(log_directory): os.makedirs(log_directory)
        timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
        log_file_path = os.path.join(log_directory, f'{timestamp}-munpia-ctas-log.json')
        with open(log_file_path, 'w', encoding='utf-8') as f:
            json.dump(update_log, f, ensure_ascii=False, indent=4, default=default_serializer)
        print(f"변경로그 저장: {log_file_path}")


def store_db_munpia_pg_copy(json_path='munpia_novel_info.json'):
    """임시 테이블 + COPY 명령을 활용한 초고속 업데이트"""
    novel_list = load_munpia_data(json_path)
    start_time = time.time()
    dt = datetime.now()

    with Session() as session:
        db_novels_dict = {n.id: n for n in session.query(Munpia)}
        db_ids = set(db_novels_dict.keys())
        print(f"DB에 {len(db_ids)}개 데이터 존재")

        insert_data, update_data, update_log = [], [], []
        field_map = {
            "platform": "platform",
            "title": "title",
            "info": "info",
            "author": "author",
            "href": "location",
            "thumbnail": "thumbnail",
            "tag": "tags",
            "the_number_of_serials": "chapter",
            "view": "views",
            "newstatus": "newstatus",
            "finishstatus": "finishstatus",
            "agegrade": "agegrade",
            "registdate": "registdate",
            "updatedate": "updatedate"
        }
        for novel in novel_list:
            n_id = novel.get("id")
            if n_id is None: continue
            is_new = n_id not in db_ids
            db_novel = None if is_new else db_novels_dict[n_id]
            payload = {'id': n_id}
            changes = {}
            for json_key, orm_key in field_map.items():
                new_val = novel.get(json_key)
                old_val = None if is_new else getattr(db_novel, orm_key)
                if orm_key in ["chapter", "views"]:
                    new_val = int(new_val or 0)
                if orm_key in ["newstatus", "finishstatus", "agegrade"]:
                    new_val = bool(new_val) if new_val is not None else False
                if orm_key in ["registdate", "updatedate"]:
                    if new_val and not isinstance(new_val, datetime):
                        try:
                            new_val = datetime.fromisoformat(new_val)
                        except Exception:
                            new_val = dt
                    
                    # 날짜 비교 시 시간대 제거하여 정확한 비교
                    if not is_new and old_val is not None and new_val is not None:
                        # 기존 값에서 날짜 부분만 추출 (시간대 제거)
                        old_date = old_val.replace(tzinfo=None).date()
                        new_date = new_val.replace(tzinfo=None).date()
                        if old_date == new_date:
                            continue  # 날짜가 같으면 변경사항으로 간주하지 않음
                if is_new or old_val != new_val:
                    payload[orm_key] = new_val
                    if not is_new:
                        # info 등 텍스트 필드는 clean_text로 정제해서 비교 및 로그 기록
                        if orm_key == "info":
                            before_clean = clean_text(old_val)
                            after_clean = clean_text(new_val)
                            if before_clean != after_clean:
                                changes[orm_key] = {
                                    "before": before_clean,
                                    "after": after_clean
                                }
                        else:
                            if str(old_val) != str(new_val):
                                changes[orm_key] = {"before": str(old_val), "after": str(new_val)}
            if is_new:
                payload.setdefault('crawltime', dt)
                insert_data.append(payload)
            elif changes:
                payload['crawltime'] = dt
                for col in Munpia.__table__.columns:
                    if col.key not in payload:
                        payload[col.key] = getattr(db_novel, col.key)
                if payload.get('author') is None:
                    payload['author'] = 'Unknown'
                if payload.get('title') is None:
                    payload['title'] = 'Unknown Title'
                if payload.get('platform') is None:
                    payload['platform'] = 'Munpia'
                update_data.append(payload)
                update_log.append({"ID": n_id, "Changes": changes})
        print(f"신규 {len(insert_data)}건, 업데이트 {len(update_data)}건")
        unique_update_data = {}
        for row in update_data:
            unique_update_data[row['id']] = row
        update_data = list(unique_update_data.values())
        print(f"중복 제거 후 업데이트 {len(update_data)}건")
        try:
            if insert_data:
                print(f"신규 데이터 삽입 시작... ({len(insert_data)}건)")
                for i, batch in enumerate(chunked(insert_data, BATCH_SIZE)):
                    session.bulk_insert_mappings(Munpia, batch)
                    print(f"  신규 데이터 배치 {i+1} 완료 ({len(batch)}건)")
            if update_data:
                print(f"임시 테이블 + COPY 명령으로 업데이트 시작...")
                temp_table_name = f"temp_munpia_copy_{int(time.time())}"
                create_temp_table_sql = f"""
                    CREATE TEMP TABLE {temp_table_name} (
                        id BIGINT PRIMARY KEY,
                        platform TEXT,
                        title TEXT,
                        info TEXT,
                        author TEXT,
                        location TEXT,
                        thumbnail TEXT,
                        tags TEXT,
                        chapter BIGINT,
                        views BIGINT,
                        newstatus BOOLEAN,
                        finishstatus BOOLEAN,
                        agegrade BOOLEAN,
                        registdate TIMESTAMP WITH TIME ZONE,
                        updatedate TIMESTAMP WITH TIME ZONE,
                        crawltime TIMESTAMP WITH TIME ZONE
                    );
                """
                session.execute(text(create_temp_table_sql))
                print(f"임시 테이블 {temp_table_name} 생성 완료")
                columns = [
                    'id', 'platform', 'title', 'info', 'author', 'location', 'thumbnail',
                    'tags', 'chapter', 'views', 'newstatus', 'finishstatus', 'agegrade',
                    'registdate', 'updatedate', 'crawltime'
                ]
                # CSV 임시 파일 생성 및 COPY
                print(f"CSV 파일 생성 및 COPY 명령으로 {len(update_data)}건 삽입 중...")
                with tempfile.NamedTemporaryFile('w+', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
                    
                    # tqdm으로 진행률 표시
                    with tqdm(total=len(update_data), desc="CSV 생성", unit="건") as pbar:
                        for row in update_data:
                            values = []
                            for col in columns:
                                val = row.get(col, None)
                                if col in ["chapter", "views"]:
                                    try:
                                        values.append(int(val))
                                    except (ValueError, TypeError):
                                        values.append(0)
                                elif col in ["newstatus", "finishstatus", "agegrade"]:
                                    values.append('t' if val else 'f')
                                elif col in ["registdate", "updatedate", "crawltime"]:
                                    if isinstance(val, datetime):
                                        values.append(val.isoformat())
                                    elif val is None:
                                        values.append(dt.isoformat())
                                    else:
                                        try:
                                            values.append(datetime.fromisoformat(str(val)).isoformat())
                                        except Exception:
                                            values.append(dt.isoformat())
                                else:
                                    if val is None or val == '':
                                        values.append('')
                                    else:
                                        cleaned = str(val)
                                        # HTML 태그 및 특수문자 제거
                                        import re
                                        cleaned = re.sub(r'<br\s*/?>', ' ', cleaned, flags=re.IGNORECASE)
                                        cleaned = re.sub(r'<[^>]+>', '', cleaned)  # 모든 HTML 태그 제거
                                        cleaned = cleaned.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
                                        cleaned = re.sub(r'\s+', ' ', cleaned)
                                        values.append(cleaned.strip())
                            writer.writerow(values)
                            pbar.update(1)
                    
                    csvfile.flush()
                    csvfile.seek(0)
                    
                    print("COPY 명령 실행 중...")
                    conn = session.connection().connection
                    with conn.cursor() as cursor:
                        cursor.copy_expert(
                            f"COPY {temp_table_name} ({','.join(columns)}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE, ENCODING 'UTF8')",
                            csvfile
                        )
                print(f"COPY로 {len(update_data)}건 임시 테이블에 삽입 완료")
                # UPDATE JOIN
                print("UPDATE JOIN 실행 중...")
                update_join_sql = f"""
                    UPDATE munpia SET
                        platform = t.platform,
                        title = t.title,
                        info = t.info,
                        author = t.author,
                        location = t.location,
                        thumbnail = t.thumbnail,
                        tags = t.tags,
                        chapter = t.chapter,
                        views = t.views,
                        newstatus = t.newstatus,
                        finishstatus = t.finishstatus,
                        agegrade = t.agegrade,
                        registdate = t.registdate,
                        updatedate = t.updatedate,
                        crawltime = t.crawltime
                    FROM {temp_table_name} t
                    WHERE munpia.id = t.id;
                """
                result = session.execute(text(update_join_sql))
                updated_count = result.rowcount
                print(f"UPDATE JOIN 완료: {updated_count}건 업데이트")
            session.commit()
            print("DB 작업 커밋 완료")
        except Exception as e:
            print(f"[DB 작업 에러] 롤백합니다: {e}")
            session.rollback()
            raise
    end_time = time.time()
    print(f"총 {end_time - start_time:.2f}초 소요")
    if update_log:
        log_directory = 'DB_Processing_Log'
        if not os.path.exists(log_directory): os.makedirs(log_directory)
        timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
        log_file_path = os.path.join(log_directory, f'{timestamp}-munpia-copy-log.json')
        with open(log_file_path, 'w', encoding='utf-8') as f:
            json.dump(update_log, f, ensure_ascii=False, indent=4, default=default_serializer)
        print(f"변경로그 저장: {log_file_path}")


# 실행 예시

if __name__ == "__main__":
    print("데이터베이스 연결 및 기본 쿼리 테스트:")
    main_queries()
    print("-" * 40)
    
    # 두 가지 업데이트 방식 중 선택
    print("업데이트 방식을 선택하세요:")
    print("1. 병렬 개별 UPDATE (기존 방식)")
    print("2. 임시 테이블 Bulk UPDATE (새로운 방식)")
    print("3. CTAS 방식을 사용한 업데이트")
    print("4. 임시 테이블 + COPY 명령을 활용한 초고속 업데이트")
    
    #choice = input("선택 (1, 2, 3 또는 4): ").strip()
    choice = "4"
    
    if choice == "2":
        print("임시 테이블을 사용한 bulk update 방식으로 실행합니다...")
        store_db_munpia_pg_bulk_update("munpia_novel_info.json")
    elif choice == "3":
        print("CTAS 방식을 사용한 업데이트 방식으로 실행합니다...")
        store_db_munpia_pg_ctas("munpia_novel_info.json")
    elif choice == "4":
        print("임시 테이블 + COPY 명령을 활용한 초고속 업데이트 방식으로 실행합니다...")
        store_db_munpia_pg_copy("munpia_novel_info.json")
    else:
        print("병렬 개별 UPDATE 방식으로 실행합니다...")
        store_db_munpia_pg("munpia_novel_info.json")
