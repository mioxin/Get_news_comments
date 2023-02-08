import win32com.client
import pythoncom
import sys
#import urllib.parse
import json
import locale
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
try:
    import _winreg as winreg
except:
    import winreg

class HTTP:
    proxy = ""
    proxyExclude = ""
    isProxy = False

    def __init__(self):
        self.get_proxy()
        

    def get_proxy(self):
        oReg = winreg.ConnectRegistry(None, winreg.HKEY_CURRENT_USER)
        oKey = winreg.OpenKey(oReg, r'Software\Microsoft\Windows\CurrentVersion\Internet Settings')
        dwValue = winreg.QueryValueEx(oKey, 'ProxyEnable')

        if dwValue[0] == 1:
            oKey = winreg.OpenKey(oReg, r'Software\Microsoft\Windows\CurrentVersion\Internet Settings')
            dwValue = winreg.QueryValueEx(oKey, 'ProxyServer')[0]
            self.isProxy = True
            self.proxy = dwValue
        self.proxyExclude = winreg.QueryValueEx(oKey, 'ProxyOverride')[0]
        print()

    def url_get(self, url):
        pythoncom.CoInitializeEx(0)
        httpCOM = win32com.client.Dispatch('Msxml2.ServerXMLHTTP.6.0')

        if self.isProxy:
            httpCOM.setProxy(2, self.proxy, self.proxyExclude)

        httpCOM.setOption(2, 13056)
        httpCOM.open('GET', url, False)
        httpCOM.setRequestHeader('Connection', 'keep-alive')
        httpCOM.setRequestHeader('sec-ch-ua', '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"')
        httpCOM.setRequestHeader('sec-ch-ua-mobile', '?0')
        httpCOM.setRequestHeader('DNT', '1')
        httpCOM.setRequestHeader('Upgrade-Insecure-Requests', '1')
        httpCOM.setRequestHeader('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36')
        httpCOM.setRequestHeader('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9')
        httpCOM.setRequestHeader('Sec-Fetch-Site', 'same-origin')
        httpCOM.setRequestHeader('Sec-Fetch-Mode', 'navigate')
        httpCOM.setRequestHeader('Sec-Fetch-Dest', 'document')
        httpCOM.setRequestHeader("Referer", 'https://www-int.hq.bc/')
        httpCOM.setRequestHeader('Accept-Language', 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7')
        httpCOM.setRequestHeader('Cookie', 'PHPSESSID=1ef8fc044720bbc7ca1c5adf0e052310; NSC_xxx-jou-mc-443=ffffffff091900d245525d5f4f58455e445a4a423660')
        httpCOM.send()
        #print('Ready state',httpCOM.readyState )
        #print(httpCOM.getAllResponseHeaders())
        return (httpCOM.status, httpCOM.responseText)

class News:
    _count=0

    def __init__(self, list_obj):
        News._count += 1
        self.news_name = list_obj.find('a', attrs = {'class':'ln7'}).text
        self.news_href = list_obj.find('a', attrs = {'class':'ln7'}).get('href')
        self.n_id = int(self.news_href.split('id=')[1])
        news_info = list_obj.find('div', attrs = {'class':'div16'})
        list_news_info = news_info.text.split()
        date_l = list_news_info[:3]
        date_l[1] = 'май' if date_l[1][:3] == 'мая' else date_l[1][:3]
        self.news_date = datetime.strptime(" ".join(date_l), '%d %b %Y')#, %H:%M')#' '.join(list_news_info[:3])
        self.news_view = int(list_news_info[3])
        self.news_comm = int(list_news_info[6])
        self.news_likes = int(list_news_info[8])

    def txt(self):
        return (self.n_id,self.news_name, self.news_href, datetime.strftime(self.news_date, '%d %B %Y'), self.news_view, self.news_comm, self.news_likes)#,news_info.text)

    def csv(self, sep=','):
        return (f"{self.n_id}{sep}{self.news_name}{sep}{self.news_href}{sep}{datetime.strftime(self.news_date, '%d %B %Y')}{sep}{self.news_view}{sep}{self.nnews_comm}{sep}{self.news_likes}")#,news_info.text)

class Comms():
    _count = 0

    @classmethod
    def _strtodate(cls,str):
        date_l = str.split()
        if date_l[-1] == 'назад':
            tdelt = timedelta(minutes=int(date_l[0])) if date_l[1][0] == 'м' else timedelta(hours=int(date_l[0]))
            return datetime.now() - tdelt
        elif date_l[-1] == 'что': 
            return datetime.now()
        else:
            date_l[1] = 'май' if date_l[1][:3] == 'мая' else date_l[1][:3]
            return datetime.strptime(" ".join(date_l), '%d %b %Y, %H:%M')#' '.join(list_news_info[:3])

    def __init__(self, json_comm, n_id, parent = 0):
        self.id = int(json_comm['id'])
        self.tab_num = int(json_comm['tab_num'])
        self.name = json_comm['name']
        self.text = json_comm['text']
        self.date_add = Comms._strtodate(json_comm['date_add'])
        self.date_edit = Comms._strtodate(json_comm['date_edit']) if json_comm['date_edit'] else None
        self.img = json_comm['img']
        #self.children = json_comm[children]
        self.likes_count = int(json_comm['likes_count'])
        self.dislikes_count = int(json_comm['dislikes_count'])
        self.likes_total = int(json_comm['likes_total'])
        self.liked = int(json_comm['liked'])
        self.level = int(json_comm['level'])
        self.own = json_comm['own']
        self.deleted = json_comm['deleted']
        self.mention = json_comm['mention']
        self.images = json_comm['images']
        self.parent = parent
        self.n_id = n_id

    @classmethod
    def create_comm_obj(cls, json_comm, n_id, parent = 0):
        _c_list = []
        def clojure(json_comm, n_id, parent = 0):
            if json_comm['children']:
                for item in json_comm['children']:
                    clojure(item, n_id, json_comm['id'])
            c = cls(json_comm, n_id, parent)
            _c_list.append(c)
        clojure(json_comm, n_id, parent)    
        return _c_list            

    def txt(self):
        return (self.id, self.tab_num, datetime.strftime(self.date_add, '%d %B %Y, %H:%M'), 
                datetime.strftime(self.date_edit, '%d %B %Y, %H:%M') if self.date_edit else '', self.parent, self.n_id, self.name, self.text )

    def csv(self,sep = ','):
        return f'{self.id}{sep}{self.tab_num}{sep}{self.name}{sep}"{self.text}"{sep}{self.date_add}{sep}{self.date_edit}{sep}{self.img}{sep}{self.likes_count}{sep}{self.dislikes_count}{sep}{self.likes_total}{sep}{self.liked}{sep}{self.level}{sep}{self.own}{sep}{self.deleted}{sep}{self.mention}{sep}{self.images}{sep}{self.parent}{sep}{self.n_id}{sep}'

class WwwInt():
    _url = r'https://www-int.hq.bc/?type=1'
    def __init__(self):
        pass

    def get_All(self):
        pass

    def get_New(self, news_id):
        pass



def main():
    locale.setlocale(locale.LC_ALL, '') # the ru locale is installed
    #resp = reqst.('https://stepik.org/media/attachments/lesson/245130/6.html') # скачиваем файл
    r = HTTP()
    url = r'https://www-int.hq.bc/?type=1'
    dom_url = url.split('?')[0]
    newsID, commID = 0, 0
    isNextPage = True
    while isNextPage:
        print('============',url)
        html = r.url_get(url)  # считываем содержимое
        if html[0] != 200:
            print('Status code:',*html) 
            isNextPage = False
        else:    
            html = html[1]
            soup = BeautifulSoup(html, 'html.parser') # делаем суп
            #table = soup.find('tr', 'class' = True)

            list_obj = soup.find_all('td', class_ ='div18')
            soup_next_page = soup.find_all('td', class_ = 'td26')[-1]
            print(soup_next_page, '***', soup_next_page.find('a').text)
            isNextPage = soup_next_page.find('a').text == 'Туда'
            url = dom_url + soup_next_page.find('a').get('href')
            for td in list_obj: #список статей на странице
                n = News(td)
                print(f"NEWS: {n._count}")
                print(n.txt())
                n_id, comms_page = n.n_id, 0
                while True: # комментарии к статье
                    comms_page += 1
                    mod_comms = f'modules/news_view/comments.php?task=get&n_id={n_id}&cpage={comms_page}'
                    comm_json = json.loads(r.url_get(dom_url + mod_comms)[1])
                    if comm_json['comms']:
                        for c in comm_json['comms']:
                            comm = Comms.create_comm_obj(c,n_id)
                            [print(it.csv(), sep='\n') for it in comm]
                    else: break
        if n._count >5: break

if __name__ == "__main__":
    main()
