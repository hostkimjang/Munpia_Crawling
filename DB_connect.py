from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, BigInteger, Text, DateTime, String, Boolean, and_, Integer, text
from sqlalchemy.orm import declarative_base, sessionmaker
import os
from datetime import datetime
import json
import time
import io
import uuid

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


if __name__ == "__main__":
    print("데이터베이스 연결 및 기본 쿼리 테스트:")
    main_queries()
    print("-" * 40)