import asyncio
import pprint
import time
import aiohttp
import requests
import json
import random
from info import set_novel_info
from store import store_info

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest"
}

#url = f"https://mm.munpia.com/free/getList?page=1&rows=30&tab=regular&subtab=&selectbox="
#url = f"https://mm.munpia.com/main/v42/slide/free/tab/regular"

#---유료 소설 목로
# 연재 신규베스트 "https://mm.munpia.com/pl/getList?page=1&rows=30&tab=plserial&subtab=new&selectbox="
# 최신 "https://mm.munpia.com/pl/getList?page=1&rows=30&tab=plserial&subtab=serial&selectbox="
# 완결 "https://mm.munpia.com/pl/getList?page=1&rows=30&tab=plserial&subtab=serial_end&selectbox=

#---무료 소설 목록
# 작가 "https://mm.munpia.com/free/getList?page=1&rows=30&tab=pro&subtab=&selectbox="
# 일반 "https://mm.munpia.com/free/getList?page=1&rows=30&tab=regular&subtab=&selectbox="
# 자유 "https://mm.munpia.com/free/getList?page=1&rows=30&tab=free&subtab=&selectbox="
# 완결 "https://mm.munpia.com/free/getList?page=1&rows=30&tab=finish&subtab=&selectbox="

async def get_pl_sort_new_best_list(session, novel_list):
    end = False
    if end == False:
        for i in range(1, end_num):
            url = f"https://mm.munpia.com/pl/getList?page={i}&rows=30&tab=plserial&subtab=new&selectbox"
            try:
                while True:  # 무한 루프를 통해 재시도
                    async with session.get(url, headers=headers) as response:
                        if response.status == 429:  # HTTP 상태 코드 429 (Too Many Requests) 처리
                            # 대기시간을 무작위로 설정한 후 재시도
                            wait_time = random.randint(5, 10)  # 예: 5~10초 대기
                            print(f"유료 신규베스트 순회{i}회 오류")
                            print(f"HTTP 오류 429: 대기 후 재시도 ({wait_time}초 대기)")
                            await asyncio.sleep(wait_time)
                            continue  # 현재 페이지 재시도
                        elif response.status != 200:
                            print(f"HTTP 오류: {response.status}")
                            break  # 오류가 발생한 경우 현재 페이지를 스킵하고 다음 페이지로 이동
                        print(f"유료 작가 순회 {i}회")
                        page = await response.json()
                        page = page['content']['list']
                        if not page:
                            print("최대 페이지 도달")
                            end = True
                            return end
                            break
                        else:
                            for i in page:
                                novel_info = set_novel_info(platform="Munpia",
                                                            title=i['title'],
                                                            info=i['story'],
                                                            author=i['author'],
                                                            href=i['href'],
                                                            thumbnail=i['cover'],
                                                            tag=i['genreText'],
                                                            the_number_of_serials=i['sumEntry'],
                                                            view=i['sumHitText'],
                                                            newstatus=i['isNew'],
                                                            finishstatus=i['isFinish'],
                                                            agegrade=i['isAdult'],
                                                            registdate=i['registDate'],
                                                            updatedate=i['updateDate'],
                                                            sort_option=i['nvNgCode'])
                                novel_list.append(novel_info)
                            break
            except aiohttp.ClientError as e:
                print(f"{url}에서 데이터를 가져오는 중 오류 발생: {e}")


async def get_pl_sort_latest_list(session, novel_list):
    end = False
    if end == False:
        for i in range(1, end_num):
            url = f"https://mm.munpia.com/pl/getList?page={i}&rows=30&tab=plserial&subtab=serial&selectbox="
            try:
                while True:  # 무한 루프를 통해 재시도
                    async with session.get(url, headers=headers) as response:
                        if response.status == 429:  # HTTP 상태 코드 429 (Too Many Requests) 처리
                            # 대기시간을 무작위로 설정한 후 재시도
                            wait_time = random.randint(5, 10)  # 예: 5~10초 대기
                            print(f"유료 최신 순회{i}회 오류")
                            print(f"HTTP 오류 429: 대기 후 재시도 ({wait_time}초 대기)")
                            await asyncio.sleep(wait_time)
                            continue  # 현재 페이지 재시도
                        elif response.status != 200:
                            print(f"HTTP 오류: {response.status}")
                            break  # 오류가 발생한 경우 현재 페이지를 스킵하고 다음 페이지로 이동
                        print(f"유료 최신 순회 {i}회")
                        page = await response.json()
                        page = page['content']['list']
                        if not page:
                            print("최대 페이지 도달")
                            end = True
                            return end
                            break
                        else:
                            for i in page:
                                novel_info = set_novel_info(platform="Munpia",
                                                            title=i['title'],
                                                            info=i['story'],
                                                            author=i['author'],
                                                            href=i['href'],
                                                            thumbnail=i['cover'],
                                                            tag=i['genreText'],
                                                            the_number_of_serials=i['sumEntry'],
                                                            view=i['sumHitText'],
                                                            newstatus=i['isNew'],
                                                            finishstatus=i['isFinish'],
                                                            agegrade=i['isAdult'],
                                                            registdate=i['registDate'],
                                                            updatedate=i['updateDate'],
                                                            sort_option=i['nvNgCode'])
                                novel_list.append(novel_info)
                            break
            except aiohttp.ClientError as e:
                print(f"{url}에서 데이터를 가져오는 중 오류 발생: {e}")

async def get_pl_sort_end_list(session, novel_list):
    end = False
    if end == False:
        for i in range(1, end_num):
            url = f"https://mm.munpia.com/pl/getList?page={i}&rows=30&tab=plserial&subtab=serial_end&selectbox="
            try:
                while True:  # 무한 루프를 통해 재시도
                    async with session.get(url, headers=headers) as response:
                        if response.status == 429:  # HTTP 상태 코드 429 (Too Many Requests) 처리
                            # 대기시간을 무작위로 설정한 후 재시도
                            wait_time = random.randint(5, 10)  # 예: 5~10초 대기
                            print(f"유료 완결 순회{i}회 오류")
                            print(f"HTTP 오류 429: 대기 후 재시도 ({wait_time}초 대기)")
                            await asyncio.sleep(wait_time)
                            continue  # 현재 페이지 재시도
                        elif response.status != 200:
                            print(f"HTTP 오류: {response.status}")
                            break  # 오류가 발생한 경우 현재 페이지를 스킵하고 다음 페이지로 이동
                        print(f"유료 완결 순회 {i}회")
                        page = await response.json()
                        page = page['content']['list']
                        if not page:
                            print("최대 페이지 도달")
                            end = True
                            return end
                            break
                        else:
                            for i in page:
                                novel_info = set_novel_info(platform="Munpia",
                                                            title=i['title'],
                                                            info=i['story'],
                                                            author=i['author'],
                                                            href=i['href'],
                                                            thumbnail=i['cover'],
                                                            tag=i['genreText'],
                                                            the_number_of_serials=i['sumEntry'],
                                                            view=i['sumHitText'],
                                                            newstatus=i['isNew'],
                                                            finishstatus=i['isFinish'],
                                                            agegrade=i['isAdult'],
                                                            registdate=i['registDate'],
                                                            updatedate=i['updateDate'],
                                                            sort_option=i['nvNgCode'])
                                novel_list.append(novel_info)
                            break
            except aiohttp.ClientError as e:
                print(f"{url}에서 데이터를 가져오는 중 오류 발생: {e}")


async def get_free_sort_author_list(session, novel_list):
    end = False
    if end == False:
        for i in range(1, end_num):
            url = f"https://mm.munpia.com/free/getList?page={i}&rows=30&tab=pro&subtab=&selectbox="
            try:
                while True:  # 무한 루프를 통해 재시도
                    async with session.get(url, headers=headers) as response:
                        if response.status == 429:  # HTTP 상태 코드 429 (Too Many Requests) 처리
                            # 대기시간을 무작위로 설정한 후 재시도
                            wait_time = random.randint(5, 10)  # 예: 5~10초 대기
                            print(f"무료 작가 순회{i}회 오류")
                            print(f"HTTP 오류 429: 대기 후 재시도 ({wait_time}초 대기)")
                            await asyncio.sleep(wait_time)
                            continue  # 현재 페이지 재시도
                        elif response.status != 200:
                            print(f"HTTP 오류: {response.status}")
                            break  # 오류가 발생한 경우 현재 페이지를 스킵하고 다음 페이지로 이동
                        print(f"무료 작가 순회 {i}회")
                        page = await response.json()
                        page = page['content']['list']
                        if not page:
                            print("최대 페이지 도달")
                            end = True
                            return end
                            break
                        else:
                            for i in page:
                                novel_info = set_novel_info(platform="Munpia",
                                                            title=i['title'],
                                                            info=i['story'],
                                                            author=i['author'],
                                                            href=i['href'],
                                                            thumbnail=i['cover'],
                                                            tag=i['genreText'],
                                                            the_number_of_serials=i['sumEntry'],
                                                            view=i['sumHitText'],
                                                            newstatus=i['isNew'],
                                                            finishstatus=i['isFinish'],
                                                            agegrade=i['isAdult'],
                                                            registdate=i['registDate'],
                                                            updatedate=i['updateDate'],
                                                            sort_option=i['nvNgCode'])
                                novel_list.append(novel_info)
                            break
            except aiohttp.ClientError as e:
                print(f"{url}에서 데이터를 가져오는 중 오류 발생: {e}")

async def get_free_sort_regular_list(session, novel_list):
    end = False
    if end == False:
        for i in range(1, end_num):
            url = f"https://mm.munpia.com/free/getList?page={i}&rows=30&tab=regular&subtab=&selectbox="
            try:
                while True:  # 무한 루프를 통해 재시도
                    async with session.get(url, headers=headers) as response:
                        if response.status == 429:  # HTTP 상태 코드 429 (Too Many Requests) 처리
                            # 대기시간을 무작위로 설정한 후 재시도
                            wait_time = random.randint(5, 10)  # 예: 5~10초 대기
                            print(f"무료 일반 순회{i}회 오류")
                            print(f"HTTP 오류 429: 대기 후 재시도 ({wait_time}초 대기)")
                            await asyncio.sleep(wait_time)
                            continue  # 현재 페이지 재시도
                        elif response.status != 200:
                            print(f"HTTP 오류: {response.status}")
                            break  # 오류가 발생한 경우 현재 페이지를 스킵하고 다음 페이지로 이동
                        print(f"무료 일반 순회 {i}회")
                        page = await response.json()
                        page = page['content']['list']
                        if not page:
                            print("최대 페이지 도달")
                            end = True
                            return end
                            break
                        else:
                            for i in page:
                                novel_info = set_novel_info(platform="Munpia",
                                                            title=i['title'],
                                                            info=i['story'],
                                                            author=i['author'],
                                                            href=i['href'],
                                                            thumbnail=i['cover'],
                                                            tag=i['genreText'],
                                                            the_number_of_serials=i['sumEntry'],
                                                            view=i['sumHitText'],
                                                            newstatus=i['isNew'],
                                                            finishstatus=i['isFinish'],
                                                            agegrade=i['isAdult'],
                                                            registdate=i['registDate'],
                                                            updatedate=i['updateDate'],
                                                            sort_option=i['nvNgCode'])
                                novel_list.append(novel_info)
                            break
            except aiohttp.ClientError as e:
                print(f"{url}에서 데이터를 가져오는 중 오류 발생: {e}")

async def get_free_sort_free_list(session, novel_list):
    end = False
    if end == False:
        for i in range(1, end_num):
            url = f"https://mm.munpia.com/free/getList?page={i}&rows=30&tab=free&subtab=&selectbox="
            try:
                while True:  # 무한 루프를 통해 재시도
                    async with session.get(url, headers=headers) as response:
                        if response.status == 429:  # HTTP 상태 코드 429 (Too Many Requests) 처리
                            # 대기시간을 무작위로 설정한 후 재시도
                            wait_time = random.randint(5, 10)  # 예: 5~10초 대기
                            print(f"무료 자유 순회{i}회 오류")
                            print(f"HTTP 오류 429: 대기 후 재시도 ({wait_time}초 대기)")
                            await asyncio.sleep(wait_time)
                            continue  # 현재 페이지 재시도
                        elif response.status != 200:
                            print(f"HTTP 오류: {response.status}")
                            break  # 오류가 발생한 경우 현재 페이지를 스킵하고 다음 페이지로 이동

                        print(f"무료 자유 순회 {i}회")
                        page = await response.json()
                        page = page['content']['list']
                        if not page:
                            print("최대 페이지 도달")
                            end = True
                            return end
                            break
                        else:
                            for i in page:
                                novel_info = set_novel_info(platform="Munpia",
                                                            title=i['title'],
                                                            info=i['story'],
                                                            author=i['author'],
                                                            href=i['href'],
                                                            thumbnail=i['cover'],
                                                            tag=i['genreText'],
                                                            the_number_of_serials=i['sumEntry'],
                                                            view=i['sumHitText'],
                                                            newstatus=i['isNew'],
                                                            finishstatus=i['isFinish'],
                                                            agegrade=i['isAdult'],
                                                            registdate=i['registDate'],
                                                            updatedate=i['updateDate'],
                                                            sort_option=i['nvNgCode'])
                                novel_list.append(novel_info)
                            break
            except aiohttp.ClientError as e:
                print(f"{url}에서 데이터를 가져오는 중 오류 발생: {e}")

async def get_free_sort_end_list(session, novel_list):
    end = False
    if end == False:
        for i in range(1, end_num):
            url = f"https://mm.munpia.com/free/getList?page={i}&rows=30&tab=finish&subtab=&selectbox="
            try:
                while True:  # 무한 루프를 통해 재시도
                    async with session.get(url, headers=headers) as response:
                        if response.status == 429:  # HTTP 상태 코드 429 (Too Many Requests) 처리
                            # 대기시간을 무작위로 설정한 후 재시도
                            wait_time = random.randint(5, 10)  # 예: 5~10초 대기
                            print(f"무료 완결 순회{i}회 오류")
                            print(f"HTTP 오류 429: 대기 후 재시도 ({wait_time}초 대기)")
                            await asyncio.sleep(wait_time)
                            continue  # 현재 페이지 재시도
                        elif response.status != 200:
                            print(f"HTTP 오류: {response.status}")
                            break  # 오류가 발생한 경우 현재 페이지를 스킵하고 다음 페이지로 이동
                        print(f"무료 완결 순회 {i}회")
                        page = await response.json()
                        page = page['content']['list']
                        if not page:
                            print("최대 페이지 도달")
                            end = True
                            return end
                            break
                        else:
                            for i in page:
                                novel_info = set_novel_info(platform="Munpia",
                                                            title=i['title'],
                                                            info=i['story'],
                                                            author=i['author'],
                                                            href=i['href'],
                                                            thumbnail=i['cover'],
                                                            tag=i['genreText'],
                                                            the_number_of_serials=i['sumEntry'],
                                                            view=i['sumHitText'],
                                                            newstatus=i['isNew'],
                                                            finishstatus=i['isFinish'],
                                                            agegrade=i['isAdult'],
                                                            registdate=i['registDate'],
                                                            updatedate=i['updateDate'],
                                                            sort_option=i['nvNgCode'])
                                novel_list.append(novel_info)
                            break
            except aiohttp.ClientError as e:
                print(f"{url}에서 데이터를 가져오는 중 오류 발생: {e}")




#response = requests.get(url=url, headers=headers)
#page = response.json()

#pprint.pprint(page, sort_dicts=False)
#time.sleep(100)
""""
page = page['content']['list']

for i in page:
    pprint.pprint(i['title'])
    pprint.pprint(i['story'])
    pprint.pprint(i['author'])
    pprint.pprint(i['href'])
    pprint.pprint(i['cover'])
    pprint.pprint(i['genreText'])
    pprint.pprint(i['sumEntry'])
    pprint.pprint(i['sumHitText'])
    pprint.pprint(i['isNew'])
    pprint.pprint(i['isFinish'])
    pprint.pprint(i['isAdult'])
    pprint.pprint(i['registDate'])
    pprint.pprint(i['updateDate'])
    pprint.pprint(i['nvNgCode'])
    time.sleep(3)
"""

async def main_async():
    async with aiohttp.ClientSession() as session:
        await asyncio.gather(get_free_sort_author_list(session, novel_list), get_free_sort_regular_list(session, novel_list), get_free_sort_free_list(session, novel_list), get_free_sort_end_list(session, novel_list))
        await asyncio.gather(get_pl_sort_new_best_list(session, novel_list), get_pl_sort_latest_list(session, novel_list), get_pl_sort_end_list(session, novel_list))

end_num = 10000
novel_list = []
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
loop.run_until_complete(main_async())
loop.close()
store_info(novel_list)