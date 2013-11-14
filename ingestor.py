'''
Universal Sports Ingestor Script
Author: ikenticus
Created: 2013/11/11
'''

import os
import re
import sys
import base64
import futures
import requests
import ConfigParser
from lxml import etree
from pprint import pprint
from datetime import datetime, timedelta


class Ingestor:
    def __init__(self, args):
        self.cmd = os.path.basename(args[0])
        self.debug = False
        self.start = {}
        self.config = {}

        # retrieve conf file using current script name
        cfdir = os.path.dirname(args[0])
        if cfdir:
            cfdir += '/'
        self.conf = cfdir + 'conf/'
        cfname = self.conf + self.cmd.replace('.py', '.cf')

        if os.path.exists(cfname):
            # Read from configuration file
            config = ConfigParser.ConfigParser()
            config.read(cfname)
            for s in config.options('settings'):
                setattr(self, s, config.get('settings', s))
            self.config['base'] = config
            for feed in self.feeds.split(','):
                self.config_clone(feed)
        else:
            sys.stderr.write('Config file [%s] not found!\n' % cfname)
            sys.exit(2)

        if len(args) > 1 and args[1] == 'help':
            self.show_help()
        self.args = args[1:]


    def show_help(self):
        sys.stderr.write('''\nUsage: %s <command>
        \n    Currently, no commands.  All configuration settings are stored in conf/*.cf
        \n''' % self.cmd)
        sys.exit(1)
        

    def config_clone(self, key):
        if key not in self.config:
            self.config[key] = self.config['base']


    def get_site(self, conf, last='', stream=False, url=None):
        headers = { 'Authorization': 'Basic %s' % base64.encodestring('%s:%s' % (
            conf.get('settings', 'user'), conf.get('settings', 'pass'))) }
        if stream:
            url = conf.get('settings', 'site') + stream
            return requests.get(url, headers=headers, stream=True)
        elif url:
            return requests.get(url, headers=headers)
        else:
            url = conf.get('settings', 'site') + conf.get('settings', 'path') + last
            return requests.get(url, headers=headers)


    def get_list(self, data, regex):
        return re.findall(regex, data)


    def get_file(self, feed, file):
        if self.debug:
            sys.stderr.write('[%s] Saving %s\n' % (feed, file))
        with open('%s%s/%s' % (self.tempdir, feed, os.path.basename(file)), 'wb') as handle:
            for block in self.get_site(self.config[feed], stream=file).iter_content(1024):
                if not block:
                    break
                handle.write(block)


    def get_files(self, feed, files, data):
        for file in files:
            self.get_file(feed, file)


    def get_files_concurrent(self, feed, files, data, workers):
        with futures.ThreadPoolExecutor(max_workers = workers) as e:
            for file in files:
                e.submit(self.get_file, feed, file)


    def write_last(self, feed):
        if feed in self.start:
            file = open('last_'+feed, 'w')
            file.write(self.start[feed])
            file.close()


    def get_workers(self, feed, section='settings'):
        max_workers = 0
        if 'max_workers' in self.config[feed].options(section):
            max_workers = int(self.config[feed].get(section, 'max_workers'))
        return max_workers


    def get_file_list(self, feed, last):
        cnt = 0
        data = []
        page = 'start'
        while page:
            cnt += 1
            if self.debug:
                sys.stderr.write('[%s] Retrieving file list page %d\n' % (feed, cnt))
            if page == 'start':
                data.append(self.get_site(self.config[feed], last).text)
            else:
                data.append(self.get_site(self.config[feed], url=page[0]).text)
            page = re.findall(self.config[feed].get('settings', 'page'), data[-1])
        return ''.join(data)


    def retrieve_files(self, feed):
        self.config_clone(feed)
        self.config[feed].read('%sfeeds/%s.cf' % (self.conf, feed))
        if not os.path.isdir('%s/%s' % (self.tempdir, feed)):
            os.makedirs('%s/%s' % (self.tempdir, feed))
        if os.path.isfile('last_'+feed):
            last = open('last_'+feed).read()
        else:
            last_default = [self.config[feed].get('settings', 'last_default').split()[:2]]
            last_dict = dict((fmt,float(amt)) for amt,fmt in last_default)
            last = (datetime.now() - timedelta(**last_dict)).strftime(self.config[feed].get('settings', 'last'))
        self.start[feed] = datetime.now().strftime(self.config[feed].get('settings', 'last'))

        data = self.get_file_list(feed, last)
        files = self.get_list(data, self.config[feed].get('settings', 'list'))
        if len(files) == 0:
            sys.stderr.write('[%s] No new files to retrieve\n' % feed)
        else:
            max_workers = self.get_workers(feed)
            if max_workers > 1:
                if self.debug:
                    sys.stderr.write('[%s] Retrieving files concurrently with %d workers\n' % (feed, max_workers))
                self.get_files_concurrent(feed, files, data, max_workers)
            else:
                if self.debug:
                    sys.stderr.write('[%s] Retrieving files individually\n' % feed)
                self.get_files(feed, files, data)


    def get_classes(self, feed, extra=False):
        classes = self.config[feed].get('settings', self.config[feed].get('settings', 'classify')).split(',')
        if extra:
            classes.extend(['skipped', 'failed'])
        return classes


    def get_xpath_safe(self, root, xpath):
        try:
            return root.xpath(xpath)[0]
        except:
            return None


    def get_xpath_check(self, root, xpaths):
        value = None
        if ',' in xpaths:
            for xpath in xpaths.split(','):
                value = self.get_xpath_safe(root, xpath)
                if value:
                    break
        else:
            value = self.get_xpath_safe(root, xpaths)
        return value


    def bucket_file(self, feed, classes, feedpath, file):
        fullpath = feedpath + file
        if not os.path.isfile(fullpath):
            return
        if os.path.getsize(fullpath) == 0:
            os.remove(fullpath)
        else:
            root = etree.parse(fullpath)
            cls = self.get_xpath_check(root, self.config[feed].get('settings', 'xpath'))
            if self.debug:
                sys.stderr.write('[%s] Classifying %s\n' % (feed, file))
            if cls in classes:
                os.rename(fullpath, feedpath + cls + '/' + file)
            else:
                os.rename(fullpath, feedpath + 'skipped/' + file)


    def bucket_files(self, feed, files, classes, feedpath):
        for file in files:
            self.bucket_file(classes, feedpath, file)


    def bucket_files_concurrent(self, feed, files, classes, feedpath):
        with futures.ThreadPoolExecutor(max_workers = self.config[feed].get('settings', 'max_workers')) as e:
            for file in files:
                e.submit(self.bucket_file, feed, classes, feedpath, file)


    def classify_files(self, feed):
        self.config_clone(feed)
        self.config[feed].read('%sfeeds/%s.cf' % (self.conf, feed))
        feedpath = self.tempdir + feed + '/'

        classes = self.get_classes(feed, extra=True)
        for c in classes:
            if not os.path.isdir(feedpath + c):
                os.makedirs(feedpath + c) 

        files = os.listdir(feedpath)
        if len(files) == 0:
            sys.stderr.write('[%s] No files to classify\n' % feedpath)
        else:
            max_workers = self.get_workers(feed)
            if max_workers > 1:
                if self.debug:
                    sys.stderr.write('[%s] Classifying files concurrently with %d workers\n' % (feed, max_workers))
                self.bucket_files_concurrent(feed, files, classes, feedpath)
            else:
                if self.debug:
                    sys.stderr.write('[%s] Classifying files individually\n' % feed)
                self.bucket_files(feed, files, classes, feedpath)


    def failure_check(self, feed, value, file, desc='unknown'):
        if 'fail' in self.config[feed].options('settings'):
            for fail in self.config[feed].get('settings', 'fail').split(','):
                if value and fail in value:
                    if self.debug:
                        sys.stderr.write('[%s] Failed %s check: %s\n' % (feed, desc, file))
                    os.rename(file, '%s%s/failed/%s' % (self.tempdir,
                        file.replace(self.tempdir, '').split('/')[0], os.path.basename(file)))


    def combine_file(self, feed, fullpath):
        root = etree.parse(fullpath)
        group = self.get_xpath_check(root, self.config[feed].get('combine', 'group'))
        order = self.get_xpath_check(root, self.config[feed].get('combine', 'order'))
        return {'group': group, 'order': order, 'file': fullpath}


    def combine_files(self, feed, filepath):
        pool = []
        for file in os.listdir(filepath):
            pool.append(self.combine_file(feed, filepath + '/' + file))
        return pool


    def combine_files_concurrent(self, feed, filepath, workers):
        pool = []
        with futures.ThreadPoolExecutor(max_workers = workers) as e:
            for file in os.listdir(filepath):
                pool.append(e.submit(self.combine_file, feed, filepath + '/' + file))
            if len(pool) == 0:
                sys.stderr.write('[%s] No files to parse\n' % feedpath)
            return [ p.result() for p in pool ]


    def keep_latest(self, feed, filelist, fullpath):
        keep = {}
        for s in filelist:
            if s['group'] not in keep or keep[s['group']]['order'] < s['order']:
                keep[s['group']] = s
        keepers = [ keep[k]['file'] for k in keep ]
        for file in os.listdir(fullpath):
            filepath = '%s/%s' % (fullpath, file)
            if filepath not in keepers:
                if self.debug:
                    sys.stderr.write('[%s] Removing outdated %s\n' % (feed, file))
                os.remove(filepath)

    def parse_file(self, feed, fullpath):
        static = {}
        if self.debug:
            sys.stderr.write('[%s] Parsing %s\n' % (feed, fullpath))
        root = etree.parse(fullpath)
        for s in self.config[feed].options('static'):
            static[s] = self.get_xpath_check(root, self.config[feed].get('static', s))
            self.failure_check(feed, static[s], fullpath, 'static')
        return static
        steps = [ s for s in self.config[feed].sections() if re.match('\d',s) ]


    def parse_files(self, feed, filepath):
        pool = []
        for file in os.listdir(filepath):
            pool.append(self.parse_file(feed, filepath + '/' + file))
        pprint (pool)


    def parse_files_concurrent(self, feed, filepath, workers):
        pool = []
        with futures.ThreadPoolExecutor(max_workers = workers) as e:
            for file in os.listdir(filepath):
                pool.append(e.submit(self.parse_file, feed, filepath + '/' + file))
            if len(pool) == 0:
                sys.stderr.write('[%s] No files to parse\n' % feedpath)
            pprint ([ p.result() for p in pool ])


    def parse_class(self, feed, cls, feedpath):
        conf = '%s%s/%s.cf' % (self.conf, self.config[feed].get('settings', 'classify'), cls)
        max_workers = self.get_workers(feed)
        if os.path.isfile(conf):
            self.config_clone(feed)
            self.config[feed].read(conf)
            if max_workers > 1:
                if self.debug:
                    sys.stderr.write('[%s] Parsing files concurrently with %d workers\n' % (feed, max_workers))
                if 'combine' in self.config[feed].sections():
                    self.keep_latest(feed, self.combine_files_concurrent(feed, feedpath + cls, max_workers), feedpath + cls)
                self.parse_files_concurrent(feed, feedpath + cls, max_workers)
            else:
                if self.debug:
                    sys.stderr.write('[%s] Parsing files individually\n' % feed)
                if 'combine' in self.config[feed].sections():
                    self.keep_latest(feed, self.combine_files(feed, feedpath + cls), feedpath + cls)
                self.parse_files(feed, feedpath + cls)


    def parse_classes(self, feed, classes, feedpath):
        for cls in classes:
            self.parse_class(feed, cls, feedpath)


    def parse_classes_concurrent(self, feed, classes, feedpath):
        with futures.ThreadPoolExecutor(max_workers = len(classes)) as e:
            for cls in classes:
                e.submit(self.parse_class, feed, cls, feedpath)


    def parse_class_files(self, feed):
        self.config[feed].read('%sfeeds/%s.cf' % (self.conf, feed))
        feedpath = self.tempdir + feed + '/'
        classes = self.get_classes(feed)
        if len(classes) == 0:
            sys.stderr.write('[%s] No classes to parse\n' % feedpath)
        else:
            max_workers = self.get_workers(feed)
            if max_workers > 1:
                if self.debug:
                    sys.stderr.write('[%s] Parsing classes concurrently with %d workers\n' % (feed, max_workers))
                self.parse_classes_concurrent(feed, classes, feedpath)
            else:
                if self.debug:
                    sys.stderr.write('[%s] Parsing classes individually\n' % feed)
                self.parse_classes(feed, classes, feedpath)


    def process_feed(self, feed):
        self.retrieve_files(feed)
        self.classify_files(feed)
        self.parse_class_files(feed)
        self.write_last(feed)


    def process_feeds(self, feeds):
        for feed in feeds:
            self.process_feed(feed)


    def process_feeds_concurrent(self, feeds):
        with futures.ThreadPoolExecutor(max_workers = len(feeds)) as e:
            for feed in feeds:
                e.submit(self.process_feed, feed)


    def process(self):
        feeds = self.feeds.split(',')
        if len(feeds) == 0:
            sys.stderr.write('No feeds to process\n')
        else:
            max_workers = self.get_workers('base')
            if max_workers > 1:
                if self.debug:
                    sys.stderr.write('Processing feeds concurrently with %d workers\n' % max_workers)
                self.process_feeds_concurrent(feeds)
            else:
                if self.debug:
                    sys.stderr.write('Processing feeds individually\n')
                self.process_feeds(feeds)


if __name__ == "__main__":
    tool = Ingestor(sys.argv)
    tool.process()
