import scrapy
import os
from ..items import VideoItem
import glob
import json
import re
from scrapy.http import Request
import requests
import hashlib
import time
from ..JsonPlugin.collect_json import createjson, createtxt
import threading
import check_ip
from ..JsonPlugin import write_data
from pget.down import Downloader
import threading
import time
import subprocess
import pysftp
import hashlib
import urllib
import random
from collections import defaultdict
import tempfile
from scrapy.utils.project import get_project_settings
from scrapy import signals
from scrapy.crawler import CrawlerRunner
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError
from twisted.internet.error import TimeoutError, TCPTimedOutError
from pprint import pprint

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None 
myHostname = "115.231.236.91"
myUsername = "root"
myPassword = "haPpDTYW"
class VideoSpider(scrapy.Spider):
    name = 'pornhub'
    def __init__(self, foo=None, *args, **kwargs):
        super(VideoSpider, self).__init__(*args, **kwargs)
        self.ct_current = 1
        self.server = True
        self.threads = []
        self.header = dict(get_project_settings().get('DEFAULT_REQUEST_HEADERS'))
        self.proxies = {}
        if 'page' in kwargs:
            self.page = kwargs['page']
        else:
            self.page = 101
        with open('proxy_list.json') as f:
            tmp = defaultdict(list)
            proxy_list = json.load(f)
            for proxy in proxy_list:
                scheme = proxy['proxy_scheme']
                url = proxy['proxy']
                tmp[scheme].append({scheme: url})
            self.proxies = tmp
        self.start_urls = []
        if self.type == 'webcam':
            self.start_urls.append('https://www.pornhub.com/video?c=61&page={}'.format(self.page))
        elif self.type == 'pornstar':
            self.start_urls.append('https://www.pornhub.com/categories/pornstar?page={}'.format(self.page))
        elif self.type == 'babe':
            self.start_urls.append('https://www.pornhub.com/categories/babe?page={}'.format(self.page))
        elif self.type == 'japan':
            self.start_urls.append('https://www.pornhub.com/video?c=111&page={}'.format(self.page))
            
        createjson(self.name)
        createtxt(self.name)
        # t1 = threading.Thread(target=check_ip.run_ip)
        # t1.start()
        # t1 = threading.Thread(target=self.change_ip)
        # t1.start()
        with open('dataList.json') as f:
            self.all_data = json.load(f)
        
    def start_requests(self):
        for u in self.start_urls:
            yield scrapy.Request(u, callback=self.parse,
                                    errback=self.handle_error)
    def change_ip(self):
        while True:
            time.sleep(750)
            tmp = defaultdict(list)
            with open('proxy_list.json') as f:
                proxy_list = json.load(f)
                for proxy in proxy_list:
                    scheme = proxy['proxy_scheme']
                    url = proxy['proxy']
                    tmp[scheme].append({scheme: url})
                self.proxies = tmp
   

    def parse(self, response):
        
        lis = response.xpath('//ul[@id="videoCategory"]/li')
        _len = len(lis)
        for i in range(_len):
            wrap = lis[i].xpath('div')
            try:
                href = wrap.xpath('div[@class="thumbnail-info-wrapper clearfix"]/span[1]/a[1]/@href').extract()[0]
            except:
                continue
            title = wrap.xpath('div[@class="thumbnail-info-wrapper clearfix"]/span[1]/a[1]/text()').extract()[0]
            key = re.match(r'.*=(.*)', href)[1]
            duration = wrap.xpath('.//var[@class="duration"]//text()').extract()[0]
            _min = int(re.search(r'(\d+):\d+', duration).group(1))
            if _min < 5  or _min >= 90:
                continue
            md = hashlib.md5()
            md.update( ('@po' + key).encode(encoding='utf-8') )
            sign = md.hexdigest()
            if sign in self.all_data:
                continue
            try:
                thumb_url = wrap.xpath('div/div[2]/a/img/@data-src').extract()[0]
            except:
                thumb_url = None

            yield Request("https://www.pornhub.com/view_video.php?viewkey=" + key, 
                callback=self.parseURL,
                meta={
                     'name': sign, 
                     'thumb_url': thumb_url,
                     'videoName': title
                     },
                errback=self.handle_error)
            
        idx = int(re.search(r'page=(\d+)', response.url).group(1))
        a = re.sub(r'page=(\d+)', 'page='+str(idx+1), response.url)
        yield Request(a, callback=self.parse, errback=self.handle_error)
   
    def parseURL(self, response):
      
        content = response._body.decode("utf-8")
        result = re.search(
        '"quality":"720","videoUrl":"(.*?)"},', content)
        resolution = '720P'
        if result is None:
            result = re.search(
                '"quality":"480","videoUrl":"(.*?)"},', content)
            resolution = '480P'
        if result is None:
            result = re.search(
                '"quality":"360","videoUrl":"(.*?)"},', content)
            resolution = '360P'
        if result is None:
            return
        result = str(result.group(1).replace('\\', ''))
 
        yield VideoItem(
            image_urls=[response.meta['thumb_url']],
            video_url=result,
            path= self.name,
            Id=response.meta['name'],
            name = response.meta['videoName'],
            resolution=resolution
        )
     
        #self.process_item(_item, self)
       # return str(result.group(1).replace('\\', ''))

    def process_item(self, item, spider):
        # if item['image_urls'][0] != '':
        #     urllib.request.urlretrieve(item['image_urls'][0], 'images/' + item['name'] + '.jpg')
        #     with pysftp.Connection(host=myHostname, username=myUsername, password=myPassword, cnopts=cnopts) as sftp:
        #         sftp.put('images/' + item['name'] + '.jpg', '/datadisk/webcam/image/' + item['name'] + '.jpg')
        if 'url_video' in item:
            
            vpath = 'video/' + '{}.mp4'.format(item['name'])
            if spider.server:
              #  vpath = '/root/pornhub2/porbhub/video/{}.mp4'.format(item['name'])
                jpath = 'json/pornhub/{}.json'.format(item['name'])
            else:
                vpath = 'video/' + '{}.mp4'.format(item['name'])
                jpath = 'json/pornhub/' + item['name'] + '.json'
        
            _len = len(spider.threads)
            while _len >= 2:
                time.sleep(1)
                for d in spider.threads:
                    if not d.is_alive():
                        spider.threads.remove(d)
                _len = len(spider.threads)
            t = threading.Thread(target=self.d, args=(item, vpath, jpath, spider))
            t.start()
            spider.threads.append(t)
        return item
    def d(self, item, vpath, jpath, spider):
      
        #/usr/local/lib/python3.6/site-packages/pget
        h = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36', 
            'Referer':'https://www.google.com/'}
        # proxies=spider.proxies
        downloader = Downloader(item['url_video'], vpath, chunk_count=0, high_speed=True , headers=h, proxies=spider.proxies)
        downloader.start()
        downloader.wait_for_finish()
        sec = self.check_video(vpath)
        if not sec:
            return
    
        with open(jpath, 'w+') as f:
            _t = int(time.time())
            sig = "sc%7*g{}@!$%".format(_t)
            md = hashlib.md5()
            md.update( sig.encode(encoding='utf-8') )
            sign = md.hexdigest() # 2
            post_dict = {
                'time': _t,
                'sig': sign,
                'name': item['videoName'],
                'area': 'us',
                'cate': 'Beauty',
                'year': 2019,
                'director': '',
                'actor': '',
                'type': 'movie', 
                'total': 1,
                'cover_url':  item['name'] + '.jpg',
                'grade': 2.0,
                'mins': sec,
                'source_url':  item['name'] + '.mp4',
                'resolution': item['resolution'],
                'part': 1,
                'intro': ''
            }
            json.dump(post_dict, f)
            spider.all_data.append(item['name'])
            write_data.open_data('dataList',item['name'] + '\n')
    
    def check_video(self, file_name):
        try:
            _os = os.name
            file_name = os.path.join(os.getcwd() , file_name)
            if _os == 'nt':
                a = str(subprocess.check_output('ffprobe -i "'+ file_name +
                                        '" 2>&1 |findstr "Duration"', shell=True))
            else:
                a = str(subprocess.check_output('ffprobe -i '+file_name+' 2>&1 |grep Duration',shell=True))
                
        
            a = a.split(",")[0].split("Duration:")[1].strip()
            h, m, s = a.split(':')
            
            duration = int(h) * 3600 + int(m) * 60 + float(s)
            duration = int(duration)
            return duration
        except:
            os.remove(file_name)
            print('ddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd')
            return False
    
