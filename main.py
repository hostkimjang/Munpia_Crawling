import pprint
import time

import requests
import json

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest"
}

url = f"https://mm.munpia.com/free/getList?page=1&rows=30&tab=regular&subtab=&selectbox="
#url = f"https://mm.munpia.com/main/v42/slide/free/tab/regular"

response = requests.get(url=url, headers=headers)
page = response.json()

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
    pprint.pprint(i['registDate'])
    pprint.pprint(i['updateDate'])
    time.sleep(3)