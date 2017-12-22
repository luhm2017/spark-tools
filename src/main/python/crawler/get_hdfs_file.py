#!/usr/bin/env python
# encoding: utf-8

import getopt
import json
import requests
import sys
import importlib
importlib.reload(sys)
import datetime


class HueOperate:

    def __init__(self,sleep_sec=0, username=None, password=None):
        # init parameter
        self.next_url = '/beeswax/execute'
        self.login_url = 'http://10.16.66.2:8000/accounts/login/?next=/'
        self.username = username
        self.password = password
        self.ip = '10.16.66.2'
        self.sleep_sec = sleep_sec
        self._HueOperate__session = self.login_hue()

    #登录hue
    def login_hue(self):
        session = requests.Session()
        r = session.get(self.login_url)
        form_data = dict(username = self.username, password = self.password, csrfmiddlewaretoken = session.cookies['csrftoken'], next = self.next_url)
        try:
            r = session.post(url = self.login_url, data = form_data, cookies = dict(), headers = dict(Referer = self.login_url))
            self.cookies = session.cookies
            self.headers = session.headers
            self.headers.update({
                'X-CSRFToken': self.cookies['csrftoken'] })
        except Exception:
            return False

        return session

    # 获取文件数据
    def get_file_data(self, filepath, localdir):
        try:
            url = 'http://%s:8000/filebrowser/download=' % self.ip + filepath
            response = self._HueOperate__session.get(url, headers = self.headers,stream=True)
            # 本地保存路径
            localpath = localdir + '/' + filepath.split('/')[-1]
            print("localdir:"+localdir+" ,localpath:"+localpath)
            f = open(localpath, 'wb')
            f.write(response.content)
            f.close()
            return True
        except:
            None
            return False

    #获取文件数据 分块
    def get_file_data_chunk(self, filepath, localdir):
        try:
            url = 'http://%s:8000/filebrowser/download=' % self.ip + filepath
            response = self._HueOperate__session.get(url, headers = self.headers,stream=True)
            # 内容体总大小
            content_size = int(response.headers['content-length'])
            print('=====当前文件内容体总大小:'+str(content_size/1024)+'KB')
            # 本地保存路径
            localpath = localdir + '/' + filepath.split('/')[-1]
            # print("localdir:"+localdir+" ,localpath:"+localpath)
            # 文本流的形式保存文件
            with open(localpath, 'wb') as f:
                for chunk in response.iter_content(1024*100):
                    f.write(chunk)
                    # 显示下载量
                    #print('当前分块chunk大小'+str(len(chunk))+'KB')
                f.close()
            return True
        except Exception as e:
            print(e)
            return False

    #批量获取
    def get_batch_files(self,filedir,localdir):
        url = 'http://10.16.66.2:8000/filebrowser/view=%s?pagesize=1000&pagenum=1&filter=&sortby=name&descending=false&format=json' % filedir
        response = self._HueOperate__session.get(url, headers = self.headers)
        re_dic = json.loads(response.content)
        # 获取当前目录下所有文件
        for i in re_dic['files'][2:]:
            # 打印
            # 逐个文件处理
            print('start download hdfs file at '+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            try:
                starttime = datetime.datetime.now()
                #json数组，定位到当前文件
                filepath = i['stats']['path']
                # print('get hdfs file:' + filepath)
                # 单文件下载
                # filepath != '/user/fraudscore/dataForModel/fqz_order_performance_gz/part-00000-e8821ead-f8c2-4216-9acb-6fb9ea8d467b-c000.snappy.parquet'
                # 过滤掉已经下载的文件
                if self.get_file_data_chunk(filepath,localdir) :
                    endtime = datetime.datetime.now()
                    print('get hdfs file:' + filepath + ' succeeded at '+ datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+'cost time '+str((endtime-starttime).seconds) + ' seconds')
            except Exception as e:
                print('hdfs file:'+filepath+' , error:'+e)
                continue
        return True;
def usage():
    print("usage")

# 获取文件数据
def get_file_data(filepath, localDir, username, password):
    q = HueOperate(200, username, password)
    q.get_file_data(filepath, localDir)

# 批量获取文件数据
def get_batch_files(filedir, localDir, username, password):
    q = HueOperate(200, username, password)
    q.get_batch_files(filedir, localDir)

if __name__ == '__main__':
    localdir = 'E:/pythonCrawler/fqz_order_performance_gz/'
    #localdir = '/home/hadoop/modelData'
    filepath = ''
    filedir = ''
    # getopt模块，用来处理命令行参数，sys.argv命令行输入参数，
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], 'c:f:d:l:p:u:', ['help','filepath=','filedir=','localdir=','conf=','username=','password='])
    except getopt.GetoptError:
        sys.exit()
    for (name, value) in opts:
        if name == '--help':
            usage()
        if name in ('-f', '--filepath'):
            filepath = value
        if name in ('-d', '--filedir'):
            filedir = value
        if name in ('-l', '--localdir'):
            localdir = value
        if name in ('-c', '--conf'):
            conf = value
        if name in ('-u', '--username'):
            username = value
        if name in ('-p', '--password'):
            password = value
            continue
    print('================================ localdir is ' + localdir)
    print('================================= filedir is ' + filedir)
    if filepath != '':
        get_file_data(filepath, localdir, username, password)
    elif filedir != '':
        get_batch_files(filedir, localdir, username, password)
    else:
        usage()
