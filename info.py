class NovelInfo:
    def __init__(self, platform, id, title, info, author, href, thumbnail, tag, the_number_of_serials, view, newstatus, finishstatus, agegrade, registdate, updatedate, sort_option):
        self.platform = platform
        self.id = id
        self.title = title
        self.info = info
        self.author = author
        self.href = href
        self.thumbnail = thumbnail
        self.tag = tag
        self.the_number_of_serials = the_number_of_serials
        self.view = view
        self.newstatus = newstatus
        self.finishstatus = finishstatus
        self.agegrade = agegrade
        self.registdate = registdate
        self.updatedate = updatedate
        self.sort_option = sort_option


    def __str__(self):
        return f"platform: {self.platform}, " \
               f"id: {self.id}, " \
               f"title: {self.title}, " \
               f"info: {self.info}, " \
               f"author: {self.author}, " \
               f"href: {self.href}, " \
               f"thumbnail: {self.thumbnail}, " \
               f"tag: {self.tag}, " \
               f"the_number_of_serials: {self.the_number_of_serials}, " \
               f"view: {self.view}, " \
               f"newstatus: {self.newstatus}, " \
               f"finishstatus: {self.finishstatus}, " \
               f"agegrade: {self.agegrade}, " \
               f"registdate: {self.registdate}, " \
               f"updatedate: {self.updatedate}, " \
               f"sort_option: {self.sort_option}"



    def to_dict(self):
        return {
            "platform": self.platform,
            "id": self.info,
            "title": self.title,
            "info": self.info,
            "author": self.author,
            "href": self.href,
            "thumbnail": self.thumbnail,
            "tag": self.tag,
            "the_number_of_serials": self.the_number_of_serials,
            "view": self.view,
            "newstatus": self.newstatus,
            "finishstatus": self.finishstatus,
            "agegrade": self.agegrade,
            "registdate": self.registdate,
            "updatedate": self.updatedate,
            "sort_option": self.sort_option
        }

def set_novel_info(platform, id, title, info, author, href, thumbnail, tag, the_number_of_serials, view, newstatus, finishstatus, agegrade, registdate, updatedate, sort_option):
    print("-" * 100)
    print(f"platform: {platform}")
    print(f"id: {id}")
    print(f"title: {title}")
    print(f"info: {info}")
    print(f"author: {author}")
    print(f"href: {href}")
    print(f"thumbnail: {thumbnail}")
    print(f"tag: {tag}")
    print(f"the_number_of_serials: {the_number_of_serials}")
    print(f"view: {view}")
    print(f"newstatus: {newstatus}")
    print(f"finishstatus: {finishstatus}")
    print(f"agegrade: {agegrade}")
    print(f"registdate: {registdate}")
    print(f"updatedate: {updatedate}")
    print(f"sort_option: {sort_option}")
    print("-" * 100)
    return NovelInfo(platform, id, title, info, author, href, thumbnail, tag, the_number_of_serials, view, newstatus, finishstatus, agegrade, registdate, updatedate, sort_option)