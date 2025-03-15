import sqlite3
import json
import time
import os
from datetime import datetime
from pprint import pprint


def load_munpia_data():
    with open('Munpia_novel_info.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
        pprint(f"총 {len(data)}개 데이터 로드 완료")
        return data

def change_log(result):
    timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
    log_directory = 'DB_Processing_Log'

    # 디렉터리가 없으면 생성
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    log_file_path = os.path.join(log_directory, f'{timestamp}-log.json')

    def datetime_convert(obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        raise TypeError(f'Type {type(obj)} not supported.')

    with open(log_file_path, 'w', encoding='utf-8') as f :
        json.dump(result, f, ensure_ascii=False, indent=4, default=datetime_convert)


def store_db():
    novel_list = load_munpia_data()
    conn = sqlite3.connect('munpia_novel.db')
    cur = conn.cursor()
    start_time = time.time()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS novel (
            id INTEGER PRIMARY KEY,
            platform TEXT,
            title TEXT,
            info TEXT,
            author TEXT,
            location TEXT,
            thumbnail TEXT,
            tags TEXT,
            chapter INTEGER,
            views INTEGER,
            newstatus BOOLEAN,
            finishstatus BOOLEAN,
            agegrade BOOLEAN,
            registdate DATETIME,
            updatedate DATETIME,
            crawltime DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    count = 1
    total = []
    dt = datetime.now()
    for novel in novel_list:
        if novel is None:
            print("데이터가 없습니다 또는 삭제, 작업이 정상으로 완료되지 않음.")
            continue

        existing_record = cur.execute("SELECT * FROM novel WHERE id=?", (novel["id"],)).fetchone()

        if existing_record:
            print(f"{novel['id']}는 이미 존재합니다. 레코드를 업데이트하거나 무시합니다.")
            column = existing_record
            changes = {}
            pprint(novel)

            fields = [
                ("platform", 1), ("title", 2), ("info", 3), ("author", 4), ("href", 5),
                ("thumbnail", 6), ("tag", 7), ("the_number_of_serials", 8), ("view", 9), ("newstatus", 10), ("finishstatus", 11),
                ("agegrade", 12), ("registdate", 13), ("updatedate", 14)
            ]

            for field, index in fields:
                if column[index] != novel[field]:
                    changes[field] = {"before": column[index], "after": novel[field]}

            if changes:
                pprint(f"변경된 사항: {changes}")
                total.append({"ID": novel["id"], "Changes": changes})

                cur.execute("""
                    UPDATE novel
                    SET title=?, info=?, author=?, location=?, thumbnail=?, tags=?, chapter=?, 
                        views=?, newstatus=?, finishstatus=?, agegrade=?, registdate=?, updatedate=?, crawltime=?
                    WHERE id=?
                """, (
                    novel["title"], novel["info"], novel["author"], novel["href"], novel["thumbnail"],
                    novel["tag"], novel["the_number_of_serials"], novel["view"], novel["newstatus"], novel["finishstatus"],
                    novel['agegrade'], novel["registdate"], novel["updatedate"], dt,
                    novel["id"]
                ))

        else:
            pprint(f"ID:{novel['id']}는 기존에 존재하지 않습니다. 새 래코드를 추가합니다.")
            cur.execute("""
                INSERT INTO novel
                (id, platform, title, info, author, location, thumbnail, tags, chapter, views, 
                newstatus, finishstatus, agegrade, registdate, updatedate, crawltime)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                novel['id'], novel["platform"], novel["title"], novel["info"], novel["author"], novel["href"],
                novel["thumbnail"], novel["tag"], novel["the_number_of_serials"], novel["view"], novel["newstatus"], novel["finishstatus"],
                novel['agegrade'], novel["registdate"], novel["updatedate"], dt
            ))
        pprint(f"{count}/{len(novel_list)}번째 데이터 저장 완료")
        count += 1


    end_time = time.time()
    pprint(f"총 {end_time - start_time:.2f}초 소요")
    pprint("데이터 저장 완료")
    conn.commit()
    conn.close()

    if total:
        change_log(total)


if __name__ == '__main__':
    store_db()