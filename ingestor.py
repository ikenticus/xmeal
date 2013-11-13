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
from datetime import datetime, timedelta


class Ingestor:
    def __init__(self, args):
        self.cmd = os.path.basename(args[0])
        self.debug = False
        self.start = {}

        # retrieve conf file using current script name
        cfdir = os.path.dirname(args[0])
        if cfdir:
            cfdir += '/'
        self.conf = cfdir + 'conf/'
        cfname = self.conf + self.cmd.replace('.py', '.cf')

        if os.path.exists(cfname):
            # Read from configuration file
            self.config = ConfigParser.ConfigParser()
            self.config.read(cfname)
            for s in self.config.options('settings'):
                setattr(self, s, self.config.get('settings', s))
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
        

    def get_site(self, conf, last='', stream=False):
        headers = { 'Authorization': 'Basic %s' % base64.encodestring('%s:%s' % (
            conf.get('settings', 'user'), conf.get('settings', 'pass'))) }
        if stream:
            url = conf.get('settings', 'site') + stream
            return requests.get(url, headers=headers, stream=True)
        else:
            url = conf.get('settings', 'site') + conf.get('settings', 'path') + last
            return requests.get(url, headers=headers)


    def get_list(self, data, regex):
        return re.findall(regex, data)


    def get_file(self, feed, file):
        if self.debug:
            sys.stderr.write('Saving %s\n' % file)
        with open('%s%s/%s' % (self.tempdir, feed, os.path.basename(file)), 'wb') as handle:
            for block in self.get_site(self.config, stream=file).iter_content(1024):
                if not block:
                    break
                handle.write(block)


    def get_files(self, feed, data):
        for file in self.get_list(data, self.config.get('settings', 'list')):
            self.get_file(feed, file)


    def get_files_concurrent(self, feed, data):
        pool = []
        with futures.ThreadPoolExecutor(max_workers = self.config.get('settings', 'max_workers')) as e:
            for file in self.get_list(data, self.config.get('settings', 'list')):
                pool.append(e.submit(self.get_file, feed, file))
            if len(pool) == 0:
                sys.stderr.write('[%s] No new files to retrieve\n' % feed)


    def write_last(self, feed):
        if feed in self.start:
            file = open('last_'+feed, 'w')
            file.write(self.start[feed])
            file.close()


    def get_workers(self, section='settings'):
        max_workers = 0
        if 'max_workers' in self.config.options(section):
            max_workers = int(self.config.get(section, 'max_workers'))
        return max_workers


    def retrieve_files(self, feed):
        self.config.read('%sfeeds/%s.cf' % (self.conf, feed))
        if not os.path.isdir('%s/%s' % (self.tempdir, feed)):
            os.makedirs('%s/%s' % (self.tempdir, feed))
        if os.path.isfile('last_'+feed):
            last = open('last_'+feed).read()
        else:
            last_default = [self.config.get('settings', 'last_default').split()[:2]]
            last_dict = dict((fmt,float(amt)) for amt,fmt in last_default)
            last = (datetime.now() - timedelta(**last_dict)).strftime(self.config.get('settings', 'last'))
        self.start[feed] = datetime.now().strftime(self.config.get('settings', 'last'))

        max_workers = self.get_workers()
        if max_workers > 1:
            if self.debug:
                sys.stderr.write('[%s] Retrieving files concurrently with %d workers\n' % (feed, max_workers))
            self.get_files_concurrent(feed, self.get_site(self.config, last).text)
        else:
            if self.debug:
                sys.stderr.write('[%s] Retrieving files individually\n' % feed)
            self.get_files(feed, self.get_site(self.config, last).text)


    def get_classes(self, feed, extra=False):
        classes = self.config.get('settings', self.config.get('settings', 'classify')).split(',')
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


    def bucket_file(self, classes, feedpath, file):
        fullpath = feedpath + file
        if not os.path.isfile(fullpath):
            return
        if os.path.getsize(fullpath) == 0:
            os.remove(fullpath)
        else:
            root = etree.parse(fullpath)
            cls = self.get_xpath_check(root, self.config.get('settings', 'xpath'))
            if self.debug:
                sys.stderr.write('Classifying %s\n' % file)
            if cls in classes:
                os.rename(fullpath, feedpath + cls + '/' + file)
            else:
                os.rename(fullpath, feedpath + 'skipped/' + file)


    def bucket_files(self, classes, feedpath):
        for file in os.listdir(feedpath):
            self.bucket_file(classes, feedpath, file)


    def bucket_files_concurrent(self, classes, feedpath):
        pool = []
        with futures.ThreadPoolExecutor(max_workers = self.config.get('settings', 'max_workers')) as e:
            for file in os.listdir(feedpath):
                pool.append(e.submit(self.bucket_file, classes, feedpath, file))
            if len(pool) == 0:
                sys.stderr.write('[%s] No files to classify\n' % feedpath)


    def classify_files(self, feed):
        feedpath = self.tempdir + feed + '/'
        self.config.read('%sfeeds/%s.cf' % (self.conf, feed))

        classes = self.get_classes(feed, extra=True)
        for c in classes:
            if not os.path.isdir(feedpath + c):
                os.makedirs(feedpath + c) 

        max_workers = self.get_workers()
        if max_workers > 1:
            if self.debug:
                sys.stderr.write('[%s] Classifying files concurrently with %d workers\n' % (feed, max_workers))
            self.bucket_files_concurrent(classes, feedpath)
        else:
            if self.debug:
                sys.stderr.write('[%s] Classifying files individually\n' % feed)
            self.bucket_files(classes, feedpath)



    def failure_check(self, value, file, desc='unknown'):
        if 'fail' in self.config.options('settings'):
            for fail in self.config.get('settings', 'fail').split(','):
                if value and fail in value:
                    if self.debug:
                        sys.stderr.write('Failed %s check: %s\n' % (desc, file))
                    os.rename(file, '%s%s/failed/%s' % (self.tempdir,
                        file.replace(self.tempdir, '').split('/')[0], os.path.basename(file)))


    def parse_class(self, cls, feedpath, clspath):
        conf = '%s%s/%s.cf' % (self.conf, clspath, cls)
        if os.path.isfile(conf):
            self.config.read(conf)
            for file in os.listdir(feedpath + cls):
                static = {}
                fullpath = feedpath + cls + '/' + file
                if self.debug:
                    sys.stderr.write('Parsing %s\n' % fullpath)
                root = etree.parse(fullpath)
                for s in self.config.options('static'):
                    static[s] = self.get_xpath_check(root, self.config.get('static', s))
                    self.failure_check(static[s], fullpath, 'static')
                #print static


    def parse_files(self, feed):
        self.config.read('%sfeeds/%s.cf' % (self.conf, feed))
        feedpath = self.tempdir + feed + '/'
        classes = self.get_classes(feed)
        for c in classes:
            self.parse_class(c, feedpath, self.config.get('settings', 'classify'))


    def process_feed(self, feed):
        self.retrieve_files(feed)
        self.classify_files(feed)
        self.parse_files(feed)
        self.write_last(feed)


    def process_feeds(self):
        for feed in self.feeds.split(','):
            self.process_feed(feed)


    def process_feeds_concurrent(self):
        pool = []
        with futures.ThreadPoolExecutor(max_workers = self.config.get('settings', 'max_workers')) as e:
            for feed in self.feeds.split(','):
                pool.append(e.submit(self.process_feed, feed))
            if len(pool) == 0:
                sys.stderr.write('No feeds to process\n')


    def process(self):
        max_workers = self.get_workers()
        if max_workers > 1:
            if self.debug:
                sys.stderr.write('Processing feeds concurrently with %d workers\n' % max_workers)
            self.process_feeds_concurrent()
        else:
            if self.debug:
                sys.stderr.write('Processing feeds individually\n')
            self.process_feeds()


if __name__ == "__main__":
    tool = Ingestor(sys.argv)
    tool.process()
