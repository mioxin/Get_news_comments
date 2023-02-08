from wwwint import *
from peewee import *
#from playhouse.mysql_ext import MySQLConnectorDatabase
from playhouse.pool import PooledMySQLDatabase
import logging
from threading import Thread, ThreadError
from queue import Queue
from multiprocessing import Pool
import traceback

MAX_DAYS = 120 #на сколько дне назад проверять новые комментарии к новостям
NEWS_ON_PAGE = 10 #новостей на одной странице, для вывода счетчика обработанных новостей
logger = logging.getLogger('peewee')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.WARN)
#mysql_db = MySQLConnectorDatabase('wwwint', user='root', password='mmm', host='127.0.0.1', port=3306)
my_db = PooledMySQLDatabase ('wwwint', max_connections=5, stale_timeout = 60, user='root', password='mmm', host='127.0.0.1', port=3306)
locale.setlocale(locale.LC_ALL, '') # the ru locale is installed

def timer(func):
    def wrap(*args, **kwargs):
        tn =  datetime.now()
        r = func(*args, **kwargs)
        print('>>>TIMER:',func.__qualname__, (datetime.now() - tn))
        return r
    return wrap


class BaseModel(Model):
    class Meta:
        database = my_db

class NewsDB(BaseModel):
    id = AutoField(unique=True, primary_key=True, index=True)
    n_id =BigIntegerField(index=True, unique=True)
    news_name = TextField()
    news_href = TextField()
    news_view = IntegerField()
    news_comm = IntegerField()
    news_likes = IntegerField()
    news_date = DateTimeField(index=True)

class CommDB(BaseModel):
    id = AutoField(unique=True, primary_key=True, index=True)
    n_id = ForeignKeyField(NewsDB, backref='comms', field='n_id', )
    comm_id = IntegerField(index=True, unique=True)
    tab_num = IntegerField(index=True)
    name = CharField()
    text = TextField()
    date_add = DateTimeField(index=True)
    date_edit = DateTimeField(null=True)
    img = CharField()
    likes_count = IntegerField()
    dislikes_count = IntegerField()
    likes_total = IntegerField()
    liked = IntegerField()
    level = IntegerField()
    own = BooleanField()
    deleted = BooleanField()
    mention = TextField()
    images = CharField()
    parent = IntegerField(index=True)

class NewsWWWitem(News):
    def get_dbrecord(self):
        return NewsDB(n_id = self.n_id,
                    news_name = self.news_name, 
                    news_href = self.news_href,
                    news_view = self.news_view, 
                    news_comm = self.news_comm, 
                    news_likes = self.news_likes,
                    news_date = self.news_date)

class CommsWWWitem(Comms):
    def get_dbrecord(self):
        return CommDB(n_id = self.n_id,
                    comm_id = self.id,
                    tab_num = self.tab_num,
                    name = self.name,
                    text = self.text,
                    date_add = self.date_add, 
                    date_edit = self.date_edit,
                    img = self.img,
                    likes_count = self.likes_count,
                    dislikes_count = self.dislikes_count,
                    likes_total = self.likes_total,
                    liked = self.liked,
                    level = self.level,
                    own = self.own,
                    deleted = self.deleted,
                    mention = self.mention,
                    images = self.images,
                    parent = self.parent)

class WwwData:

    def __init__(self, url):
        self.request = HTTP()
        self.url = url
        self.dom_url = url.split('?')[0]
        self.count = 0
        #self.next_url = ''

    def get_news(self):
        while True:
            news_comments_dict = {}
            html = self.request.url_get(self.url)  # считываем содержимое
            if html[0] != 200:
                print('>>>Status code:',*html) 
                return None
            else:    
                html = html[1]
                soup = BeautifulSoup(html, 'html.parser') # делаем суп
                list_obj = soup.find_all('td', class_ ='div18')
                soup_next_page = soup.find_all('td', class_ = 'td26')[-1]
                # print(soup_next_page, '***', soup_next_page.find('a').text)
                self.url = self.dom_url + soup_next_page.find('a').get('href')
                for td in list_obj: #список статей на странице
                    #print('<<<<<<<<',td)
                    n = NewsWWWitem(td)
                    c_l = list(self._get_comms(n))
                    news_comments_dict[n] = c_l

                yield news_comments_dict
                if soup_next_page.find('a').text != 'Туда': #последняя страница
                    return None

            if n._count >= 5000: return None


    def get_news_async(self):
        next_url = self.url
        queue_result = Queue()

        def _get_tuple_news_comms(str_html):
            #print('<<<<<<<<',str_html[80:])
            n = NewsWWWitem(str_html)
            c_l = list(self._get_comms(n))
            queue_result.put_nowait((n, c_l))
            #news_comments_dict[n] = c_l
            # return n, c_l

        while True:
            if not next_url: return None
            news_comments_dict = {}
            html = self.request.url_get(next_url)  # считываем содержимое
            if html[0] != 200:
                print('>>>Status code:',*html) 
                return None
            else:    
                html = html[1]
                soup = BeautifulSoup(html, 'html.parser') # делаем суп
                soup_next_page = soup.find_all('td', class_ = 'td26')[-1]
                next_url = None if soup_next_page.find('a').text != 'Туда' else self.dom_url + soup_next_page.find('a').get('href')
                list_obj = soup.find_all('td', class_ ='div18')

                # with Pool(20) as p:
                #         p.map(_get_tuple_news_comms, list_obj)

                # Treads v.1
                # threds = [Thread(target=(_get_tuple_news_comms), args=(td,)) for td in list_obj]
                # [thr.start() for thr in threds]
                # [thr.join() for thr in threds]

                # No Treads 
                for td in list_obj: #список статей на странице
                    k, v = _get_tuple_news_comms(td)
                    news_comments_dict[k] = v

                while not queue_result.empty():
                    k, v = queue_result.get_nowait()
                    news_comments_dict[k] = v
            yield news_comments_dict #_get_page_of_news(self.next_url)

    def get_news_pool(self, url1):
        news_comments_dict = {}
        html = self.request.url_get(url1)  # считываем содержимое
        if html[0] != 200:
            print('>>>Status code:',*html) 
            return None
        else:    
            self.count += 1
            html = html[1]
            soup = BeautifulSoup(html, 'html.parser') # делаем суп
            list_obj = soup.find_all('td', class_ ='div18')
            soup_next_page = soup.find_all('td', class_ = 'td26')[-1]
            # print(soup_next_page, '***', soup_next_page.find('a').text)
            self.url = self.dom_url + soup_next_page.find('a').get('href')
            for td in list_obj: #список статей на странице
                #print('<<<<<<<<',td)
                n = NewsWWWitem(td)
                c_l = list(self._get_comms(n))
                news_comments_dict[n] = c_l

            return news_comments_dict
            if soup_next_page.find('a').text != 'Туда': #последняя страница
                return None



    def _get_comms(self, n):
        n_id, comms_page = n.n_id, 0
        while True: # комментарии к статье
            comms_page += 1
            mod_comms = f'modules/news_view/comments.php?task=get&n_id={n_id}&cpage={comms_page}'
            comm_json = json.loads(self.request.url_get(self.dom_url + mod_comms)[1])
            if comm_json['comms']:
                for c in comm_json['comms']:
                    yield CommsWWWitem.create_comm_obj(c,n_id)
            else: return None

class DB:
    def __init__(self):#,db):
        #self.db = MySQLConnectorDatabase('wwwint', user='root', password='mmm', host='127.0.0.1', port=3306)
        #self.db = db

        NewsDB.create_table()
        CommDB.create_table()

    def load_data(self, news_item, comms_list):
        last_loaded_news_date = datetime.now()
        try:
            if not NewsDB.get_or_none(NewsDB.n_id == news_item.n_id):
                n_db = news_item.get_dbrecord()
                n_db.save()
                last_loaded_news_date = news_item.news_date
                print(f"NEWS load to DB: {n_db.get_id()}///{news_item.txt()}")
            # else:
            #     print(f">>>Статья #{news_item.n_id} уже в базе ({news_item.news_date.date()} '{news_item.news_name}')", end=' ')
        except IntegrityError as e:
            print('>>>ERROR load news:',e.__str__())
        
        comm_count = 0
        for comm in comms_list:
            try:
                for it in comm:
                    if not CommDB.get_or_none(CommDB.comm_id == it.id):
                        it.get_dbrecord().save()
                        print(f'##Статья "{news_item.news_name}" (#{news_item.n_id}) ###Коммент НЕ в базе', it.txt())
                        comm_count += 1 #len(comm)
                    else:
                        #print('######Коммент уже в базе', it.txt())
                        if last_loaded_news_date - news_item.news_date > timedelta(days=MAX_DAYS) :
                            raise TooMachDoublCommentsException
            except IntegrityError as e:
                print('>>>ERROR load comms:',e.__str__())

        if news_item.news_comm == 0:
            pass
            #print('>Нет комментариев')# перевод строки для следующей news
        else:
            # if comm_count == 0 and news_item.news_comm > 0: print(f'>Все {news_item.news_comm} комментарии уже в базе')
            if comm_count in range(1, news_item.news_comm + 1): print(f'>Загружено {comm_count} комментариев из {news_item.news_comm}') 
        #print('=========',comm_count,news_item.news_comm,'============')
    def get_data(self):
        pass

class TooMachDoublCommentsException(Exception):
    pass

www = WwwData(r'https://www-int.hq.bc/?type=1')
db = DB()

def get_load_news(url):
    # print('>START',url)
    data_dict = www.get_news_pool(url)
    count_rows = len(data_dict)
    for n, c_list in data_dict.items():
        count_rows -= 1
        #print(str(n._count - count_rows), ': ', end=" ")
        db.load_data(n, c_list)
    # print('<END',url)

def main():
    start_app = datetime.now()

    print('ASYNC START')

    try:
        urls = [f"https://www-int.hq.bc/?&type=1&page={i}" for i in range(1, 100)]#151

        with Pool(5) as p:
            print('Start pools.')
            p.map(get_load_news, urls)
            

    except TooMachDoublCommentsException:
        print(f"\nDublicate comments in more then {MAX_DAYS} days")

    print('Work time:', datetime.now() - start_app)
    print('<<<<<<<< THE END:',datetime.now(),'>>>>>>>>>')
    

def main_sync():
    print('SYNC START')
    locale.setlocale(locale.LC_ALL, '') # the ru locale is installed
    www = WwwData(r'https://www-int.hq.bc/?type=1')
    db = DB()
    
    start_app = datetime.now()

    try:
        for data_dict in www.get_news():
            count_rows = len(data_dict)
            for n, c_list in data_dict.items():
                count_rows -= 1
                print(str(n._count - count_rows), ': ', end=" ")
                db.load_data(n, c_list)

    except TooMachDoublCommentsException:
        print(f"\nDublicate comments in more then {MAX_DAYS} days")

    print('Work time:', datetime.now() - start_app)
    print('<<<<<<<< THE END:',datetime.now(),'>>>>>>>>>')

if __name__ == "__main__":
    main()

"""
    

# def main1():
#     locale.setlocale(locale.LC_ALL, '') # the ru locale is installed
#     r = HTTP()
#     url = r'https://www-int.hq.bc/?type=1'
#     dom_url = url.split('?')[0]
#     newsID, commID = 0, 0
#     isNextPage = True

#     NewsDB.create_table()
#     CommDB.create_table()
    
#     while isNextPage:
#         print('============',url)
#         html = r.url_get(url)  # считываем содержимое
#         if html[0] != 200:
#             print('Status code:',*html) 
#             isNextPage = False
#         else:    
#             html = html[1]
#             soup = BeautifulSoup(html, 'html.parser') # делаем суп
#             #table = soup.find('tr', 'class' = True)

#             list_obj = soup.find_all('td', class_ ='div18')
#             soup_next_page = soup.find_all('td', class_ = 'td26')[-1]
#             print(soup_next_page, '***', soup_next_page.find('a').text)
#             isNextPage = soup_next_page.find('a').text == 'Туда'
#             url = dom_url + soup_next_page.find('a').get('href')
#             for td in list_obj: #список статей на странице
#                 n = NewsWWWitem(td)
#                 try:
#                     n_db = n.get_dbrecord()
#                     n_db.save()
#                     print(f"NEWS: {n._count}; load to DB: {n_db.get_id()}")
#                     print(n.txt())
#                 except IntegrityError:
#                     print(f"Error save NEWS: {n._count}; load to DB: {n.n_id}")
#                 except Exception as e:
#                     print('>>>ERROR:',e.__str__())

#                 n_id, comms_page = n.n_id, 0
#                 while True: # комментарии к статье
#                     comms_page += 1
#                     mod_comms = f'modules/news_view/comments.php?task=get&n_id={n_id}&cpage={comms_page}'
#                     comm_json = json.loads(r.url_get(dom_url + mod_comms)[1])
#                     if comm_json['comms']:
#                         for c in comm_json['comms']:
#                             try:
#                                 comm = CommsWWWitem.create_comm_obj(c,n_id)
#                                 [it.get_dbrecord().save() for it in comm]
#                                 [print(it.csv(), sep='\n') for it in comm]
#                             except IntegrityError:
#                                 print(f"Error save COMMS for NEWS: {n.n_id}")
#                                 if datetime.now() - n.news_date > timedelta(days=360) : 
#                                     print(f"Dublicate comments more then {360} days")
#                                     exit()
#                             except Exception as e:
#                                 print('>>>ERROR comms:',e)#.__str__())
#                     else: break
                    
                
#         if n._count > 5: break
"""
