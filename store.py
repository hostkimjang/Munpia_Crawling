import json
import pprint

def store_info(info_list):
    with open("Munpia_novel_info.json", "w", encoding="utf-8") as f:
        novel_data = []
        for info in info_list:
            novel_dict = {
                "platform": info.platform,
                "id": info.id,
                "title": info.title,
                "info": info.info,
                "author": info.author,
                "href": info.href,
                "thumbnail": info.thumbnail,
                "tag": info.tag,
                "the_number_of_serials": int(info.the_number_of_serials.replace(',', '')),
                "view": info.view,
                "newstatus": info.newstatus,
                "finishstatus": info.finishstatus,
                "agegrade": info.agegrade,
                "registdate": info.registdate,
                "updatedate": info.updatedate,
                "sort_option": info.sort_option
            }
            novel_data.append(novel_dict)
        json.dump(novel_data, f, ensure_ascii=False, indent=4)
        count = len(info_list)
        print(f"총 {count}개의 데이터를 저장하였습니다.")
        print("store is done!")