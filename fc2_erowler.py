# -*- coding: utf-8 -*-

import requests
import re
import time
import os
from selenium import webdriver
from bs4 import BeautifulSoup
from pymongo import MongoClient
import pprint
import hashlib
import urllib
from multiprocessing import Process
import unicodedata


email_address = 'fukuta0614@yahoo.co.jp'
password = 'qwe123qwe'

visited_urls = []
driver = None
pp = pprint.PrettyPrinter(indent=4)

FOLDER_PATH = '/Volumes/HDPF-UT/FukutaFile/drama/fc2/'
hangle = re.compile(u"[\uac00-\ud7af\u3200-\u321f\u3260-\u327f\u1100-\u11ff\u3130-\u318f\uffa0-\uffdf\ua960-\ua97f\ud7b0-\ud7ff]")
MAX_DOWNLOADS = 8

def init_mongo(database,collection):
    connect = MongoClient('localhost', 27017)#, max_pool_size=None)
    db = connect[database]
    global collect
    # collect = db.movie_list
    collect = db[collection]

def login_fc2_selenium():

    global driver
    driver = webdriver.PhantomJS()
    #driver.get("https://secure.id.fc2.com/index.php?mode=login&switch_language=jp")
    driver.get("http://fc2.com/ja/login.php?ref=video")

    driver.find_element_by_name('email').send_keys(email_address)
    driver.find_element_by_name('pass').send_keys(password)
    driver.find_element_by_name("image").click()

def get_urls_of_ranking():

    def isfc2(url):

        url = url.split("&")[0]
        if re.search(r'http:\/\/video\.fc2\.com\/?j?a?\/?a?\/content\/(\w+)/?', url):
            return True
        else:
            return False

    base_url = "http://video.fc2.com/a/list.php?m=cont&page={}&type=1"
    ranking_urls = []
    for page_number in range(1,11):
        url = base_url.format(page_number)
        soup = BeautifulSoup(requests.get(url).content)
        for link in soup.find_all('a'):
            movie_url = link['href']
            if isfc2(movie_url) and not movie_url in ranking_urls:
                ranking_urls.append(movie_url)

    return ranking_urls

def get_id_and_flv_url(url):

    FC2magick = '_gGddgPfeaf_gzyr'
    match = re.search(r'http:\/\/video\.fc2\.com\/?j?a?\/?a?\/content\/(\w+)/?', url)
    if match == None:
        return None,None
    try:
        target = match.group(1)
        hash_target = (target + FC2magick).encode('utf-8')
        mini = hashlib.md5(hash_target).hexdigest()
        ginfo_url = 'http://video.fc2.com/ginfo.php?mimi=' + mini + '&v=' + target + '&upid=' + target + '&otag=1'
        
        #content = urllib.request.urlopen(ginfo_url)
        #filepath = BeautifulSoup(content.read()).p.string
        soup = BeautifulSoup(urllib.request.urlopen(ginfo_url,timeout=3).read())
        filepath = str(soup).replace(";","").split("&amp")
        flv_url =  filepath[0].split('=')[1] + '?' + filepath[1]
        
         
        return target, flv_url
    except Exception as e:
        print(e)
        return None, None

def get_title_and_flv_url(url):
    FC2magick = '_gGddgPfeaf_gzyr'
    match = re.search(r'http:\/\/video\.fc2\.com\/?j?a?\/?a?\/content\/(\w+)/?', url)
    if match is None:
        return None, None
    target = match.group(1)

    hash_target = (target + FC2magick).encode('utf-8')
    mini = hashlib.md5(hash_target).hexdigest()
    ginfo_url = 'http://video.fc2.com/ginfo.php?mimi=' + mini + '&v=' + target + '&upid=' + target + '&otag=1'
    content = urllib.request.urlopen(ginfo_url)
    filepath = BeautifulSoup(content.read()).p.string

    try:
        title = filepath.split('&')[14].split('=')[1]  # title(need encode)
        if len(title) < 4:
            title = filepath.split('&')[15].split('=')[1]
    except:
        return None,None

    flv_url =  filepath.split('&')[0].split('=')[1] + '?' + filepath.split('&')[1]

    return title, flv_url

def crawl_fc2(url,depth=0):

    if depth >= 5:
        return
    depth+=1

    driver.get(url)
    url=url.split("&")[0]
    target, flv_url = get_id_and_flv_url(url)
    if target == None or flv_url == None or target in movie_ids:
        return
    movie_ids.append(target)
    entry = {'url':url}
    time.sleep(3)
    try:
        soup = BeautifulSoup(driver.page_source.encode('utf-8'))
        entry['title'] = soup.find('h2',class_="cont_v2_hmenu04 clearfix").text
        entry['kind'] = soup.find('div',class_='cont_v2_hmenu01 clearfix').p.text
        entry['tag'] = [li.a.span.text for li in soup.find_all('li',class_='radius_all tag_lock')]
        entry['rate'] = float(re.sub(r'\W','',soup.find('strong',class_='js-good-rate').text))/100.
        entry['playing'] = int(soup.find('ul',class_='cont_v2_info_movie01').find_all('li')[0].strong.text)
        entry['fav'] = int(soup.find('ul',class_='cont_v2_info_movie01').find_all('li')[1].strong.text)
        entry['suggest'] = [li.a['title'] for li in soup.find_all('li',class_='clearfix')[1:]]
        entry['_id'] = target
        entry['flv_url'] = flv_url
        # pp.pprint(entry)
        try:
            print(depth,entry['title'])
            collect.insert(entry)
        except Exception as e:
            print(url,e)
            return

        links = [li.a['href'] for li in soup.find_all('li',class_='clearfix')[1:]]
        for link_url in links:
            crawl_fc2(link_url,depth)

    except Exception as e:
        print(e)
        return

def collect_movies_info_to_mongo():

    global movie_ids
    movie_ids = []
    for movie in collect.find({},{'_id':1}):
        movie_ids.append(movie['_id'])

    login_fc2_selenium()
    urls = get_urls_of_ranking()

    processes = set()
    while True:
        if len(processes) < MAX_DOWNLOADS and len(urls) > 0:
            url = urls.pop(0)
            p = Process(target=crawl_fc2,args=(url,0))
            p.start()
            processes.add(p)
        set_new = set()
        for process in processes:
            if process.is_alive():
                set_new.add(process)
        processes = set_new
        if len(processes) == 0:
            break
        time.sleep(3)

def set_ready():
    def check(title, rate, playing, fav):
        if hangle.search(title):
            return False
        # if  rate >= 0.75 and playing > 200000 and fav > 500:
        if fav > 2000:
            return True
        else:
            return False
    # movies = collect.find({"kind" : "すべてのユーザー"})
    movies = collect.find({'downloaded':{"$exists":0}})
    for movie in movies:
        if check(movie['title'],movie['rate'],movie['playing'],movie['fav']) and  ['人妻 熟女'] != movie['tag']:
            movie['downloaded'] = 'ready'
        else:
            movie['downloaded'] = False
        collect.save(movie)

def download_ready():

    movies = list(collect.find({'downloaded':'ready'},{'flv_url':1,'title':1}))
    processes = set()
    while True:
        if len(processes) < MAX_DOWNLOADS and len(movies) > 0:
            movie = movies.pop(0)
            p = Process(target=download_movie,args=(movie['flv_url'],movie['title']))
            p.start()
            processes.add(p)
        set_new = set()
        for process in processes:
            if process.is_alive():
                set_new.add(process)
        processes = set_new
        if len(processes) == 0:
            break
        time.sleep(3)

    # print(x)

def download_movie(url,title=None):
    if title == None:
        title, url = get_id_and_flv_url(url)

    def reporthook(*a):
        if a[2] < 80000000:
            os.remove(file_name)
            exit()

    print(title)
    file_name = FOLDER_PATH + title + ".flv"
    try:
        urllib.request.urlretrieve(url,file_name,reporthook)
    except:
        return

def temp():
    movies = collect.find({"kind":'すべてのユーザー',"downloaded":False})
    for movie in movies:
        if movie['fav'] > 3000:
            movie['downloaded'] = 'ready'
            collect.save(movie)

def move_to_directory():
    for movie in os.listdir(FOLDER_PATH):
        if movie.endswith('.flv'):
            print(movie)
            try:
                tag = collect.find({'title':unicodedata.normalize("NFC",movie[:-4])})[0]['tag'][0]
            except:
                tag = '不明'
            print(tag)
            try:
                os.mkdir(FOLDER_PATH + tag)
            except:
                pass
            os.rename(FOLDER_PATH + movie,FOLDER_PATH + tag + '/' + movie)

def movie_ihave():
    movies = []
    for dir in os.listdir(FOLDER_PATH):
        movies.extend(os.listdir(FOLDER_PATH + dir))
    return movies

def update_downloaded_flag():
    for movie in collect.find():
        movie['downloaded'] = False
        collect.save(movie)

    for movie in movie_ihave():
        try:
            print(movie)
            dict = collect.find({'title':unicodedata.normalize("NFC",movie[:-4])})[0]
            dict['downloaded'] = True
            collect.save(dict)
        except:
            continue

def remove_small_movie():
    for dir in os.listdir(FOLDER_PATH):
        for movie in os.listdir(FOLDER_PATH + dir):
            if os.path.getsize(FOLDER_PATH + dir + '/' + movie) < 70000000:
                os.remove(FOLDER_PATH + dir + '/' + movie)

def remove_dups():
    movies = list(collect.find())
    init_mongo('fc2_movie','movies')
    for i,movie in enumerate(movies[5800:]):
        print(i,end=',')
        try:
            target,flv_url = get_id_and_flv_url(movie['url'])
        except Exception as e:
            print(movie['url'],e)
            continue

        if target == None or flv_url == None:
            print(movie['url'],'No fc2')
            continue

        movie['_id'] = target
        movie['flv_url'] = flv_url
        try:
            collect.insert(movie)
        except:
            print('duplicate error')
            continue

def move_to_directory_in_order():

    for dir in os.listdir(FOLDER_PATH):
        for movie in (os.listdir(FOLDER_PATH + dir)):
            if movie.endswith('.flv'):
                print(movie)
                try:
                    refer_movie = collect.find({'title':unicodedata.normalize("NFC",movie[:-4])})[0]
                    tag = refer_movie['tag'][0]
                    fav = refer_movie['fav']
                except:
                    tag = '不明'
                    fav = 300
                if fav > 8000:
                    folder = '★★★★★/'
                elif fav > 5000:
                    folder = '★★★★☆/'
                elif fav > 2000:
                    folder = '★★★☆☆/'
                elif fav > 1000:
                    folder = '★★☆☆☆/'
                else:
                    folder = '★☆☆☆☆/'
                os.makedirs(FOLDER_PATH + folder + tag,exist_ok=True)
                os.rename(FOLDER_PATH + tag + '/' + movie,FOLDER_PATH + folder + tag + '/' + movie)

def get_all_movie_info():
    
    session = requests.session()
    login_data = {"email":"fukuta0614@yahoo.co.jp","pass":"qwe123qwe"}
    session.post("http://secure.id.fc2.com/index.php?mode=login&switch_language=jp",login_data)
     
    #login_fc2_selenium()
    def get_info(url,time,thumbnail):

        target, flv_url = get_id_and_flv_url(url)
        if target == None or flv_url == None:
            return
        try:
            entry = {'url':url}
           # driver.get(url)
            soup = BeautifulSoup(session.get(url,timeout=5).content)
            #soup = BeautifulSoup(session.get(url,timeout=5).content)
            #soup = BeautifulSoup(driver.page_source.encode("utf-8"))
            entry['title'] = soup.find('h2',class_="cont_v2_hmenu04 clearfix").text
            entry['kind'] = soup.find('div',class_='cont_v2_hmenu01 clearfix').p.text
            entry['tag'] = [li.a.span.text for li in soup.find_all('li',class_='radius_all tag_lock')]
            entry['rate'] = float(re.sub(r'\W','',soup.find('strong',class_='js-good-rate').text))/100.
            entry['playing'] = int(soup.find('ul',class_='cont_v2_info_movie01').find_all('li')[0].strong.text)
            entry['fav'] = int(soup.find('ul',class_='cont_v2_info_movie01').find_all('li')[1].strong.text)
            entry['_id'] = target
            entry['flv_url'] = flv_url
            entry['play_time'] = time
            entry['thumbnail'] = thumbnail

            print(entry["title"])	
            collect.insert(entry)
        except Exception as e:
            print(url,e)
            return

    regex = re.compile(r'全員')
    movies = []
    base_url = 'http://video.fc2.com/ja/a/movie_search.php?isadult=1&ordertype=0&usetime=0&timestart=0&timeend=0&keyword=&perpage=50&opentype=1&page={}'
    page_number =  1
    while True:
        print(page_number)
        try:
            soup = BeautifulSoup(session.get(base_url.format(page_number),timeout=5).content)
        except Exception as e:
            page_number+=1
            with open("error.txt","a") as f:
                f.write( str(e) + str(page_number))
            continue
        try:
            if soup:
                for movie in soup.find_all('div',class_="video_list_renew clearfix")[:50]:
                    try:
                        play_time = movie.find('span',class_='video_time_renew').text
                        url = movie.find('div',class_='video_info_right').h3.a['href']
                        target = re.search(r'http:\/\/video\.fc2\.com\/?j?a?\/?a?\/content\/(\w+)/?', url).group(1)
                        if regex.search(movie.find('ul',class_='video_info_upper_renew clearfix').li.text):
                            thumbnail = movie.img['src']
                            try:
                                refer_movie = collect.find({'_id':target})[0]
                                refer_movie['thumbnail'] = thumbnail
                                collect.save(refer_movie)
                            except:
                                #get_info(url,play_time)
                                movies.append((url,play_time,thumbnail))
                    except Exception as e:
                        print(e)
        except Exception as e:
            print(e)
            print('finish')

        page_number += 1
        if len(movies) > 100:
            print('start insert')
            processes = set()
            while True:
                if len(processes) < 8 and len(movies) > 0:
                    url, play_time = movies.pop(0)
                    p = Process(target=get_info,args=(url,play_time))
                    p.start()
                    processes.add(p)
                set_new = set()
                for process in processes:
                    if process.is_alive():
                        set_new.add(process)
                processes = set_new
                if len(processes) == 0:
                    break
                time.sleep(1)

def get_gingo_url():
    FC2magick = '_gGddgPfeaf_gzyr'

    for movie in collect.find():
        hash_target = (movie['_id'] + FC2magick).encode('utf-8')
        mini = hashlib.md5(hash_target).hexdigest()
        ginfo_url = 'http://video.fc2.com/ginfo.php?mimi=' + mini + '&v=' + movie['_id'] + '&upid=' + movie['_id'] + '&otag=1'
        movie['ginfo_url'] = ginfo_url
        collect.save(movie)

def main():

    collect_movies_info_to_mongo()

    set_ready()
    download_ready()
    move_to_directory()
    update_downloaded_flag()


if __name__ == '__main__':
    init_mongo('fc2_movie','movies')
    # main()
    # download_movie('')
    # move_to_directory_in_order()
    get_all_movie_info()

