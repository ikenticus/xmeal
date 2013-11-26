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
from threading import Lock, Semaphore
from datetime import datetime, timedelta


class Ingestor:
    def __init__(self, args):
        self.cmd = os.path.basename(args[0])
        self.debug = False
        self.start = {}
        self.config = {}
        self.tables = {}

        '''
            self.tables = {
                'dbo.SMG_Table': {
                    'static': {},
                    'columns': [],
                    'rows': [{}]
                }
            }
        '''

        # retrieve conf file using current script name
        cfdir = os.path.dirname(args[0])
        if cfdir:
            cfdir += '/'
        self.conf = cfdir + 'conf/'
        self.cfname = self.conf + self.cmd.replace('.py', '.cf')

        actions = False
        if len(args) > 1:
            if args[1] == 'help':
                self.show_help()
            elif args[1].endswith('.cf'):
                self.cfname = args[1]
                sys.stderr.write('%s Reading from alternate config: %s\n' % (self.get_stamp(), self.cfname))
            else:
                actions = args[1]

        if os.path.exists(self.cfname):
            # Read from configuration file
            config = ConfigParser.ConfigParser()
            config.read(self.cfname)
            for s in config.options('settings'):
                setattr(self, s, config.get('settings', s))
            self.config['base'] = config
            for feed in self.feeds.split(','):
                self.config_clone(feed)
        else:
            sys.stderr.write('%s Config file [%s] not found!\n' % (self.get_stamp(), self.cfname))
            sys.exit(2)

        if actions:
            self.actions = actions

        # database configs
        self.dbpool = {}
        self.dblock = Lock()
        self.dbflag = Semaphore()


    def show_help(self):
        sys.stderr.write('''\nUsage: %s [<action>]\n
        help         displays this help usage
        pull         retrieve "last" files(s) from all specified feeds
        sort         separates all the pulled files into classifier folders
        parse        extracts all the data from the sorted files based on the rules
        push         insert all the parsed data into the db, initiating any load sprocs
        post         instead of directly pushing to db, utilize API to POST data
        cache        push all parsed data to specified cache mechanism via template
        purge        deletes all the files in the skipped and failed folders
        parse,push   multiple actions can be specified comma-delimited
        \n    If not action specified, will execute default settings stored in %s
        \n''' % (self.cmd, self.cfname))
        sys.exit(1)
        

    def config_clone(self, key):
        if key not in self.config:
            self.config[key] = self.config['base']

    def config_feed(self, feed):
        self.config_clone(feed)
        self.config[feed].read('%sfeeds/%s.cf' % (self.conf, feed))
        self.config_path('%s/%s' % (self.tempdir, feed))

    def config_path(self, dirpath):
        if not os.path.isdir(dirpath):
            os.makedirs(dirpath)


    def db_close(self, dsn):
        for conn in self.dbpool[dsn]:
            conn.close()

    def db_connect(self, dsn):
        self.dbpool[dsn] = []
        self.dblock[dsn] = Lock()
        self.dbflag[dsn] = Semaphore()
        for pool in range(0, self.max_conns):
            conn = pyodbc.connect(self.config['base'].get('odbc', dsn), autocommit=True)
            if self.debug:
                sys.stderr.write('%s [%s] Opened pool connection %d\n' % (self.get_stamp(), dsn, pool))
            self.dbpool[dsn].append(conn)

    def db_execute(self, dsn, sql, params=[]):
        conn = self.db_get_from_pool(dsn)
        cur = conn.cursor()
        cur.execute(sql, *params)
        self.db_return_to_pool(dsn, conn)

    def db_get_from_pool(self, dsn):
        if not self.dbpool:
            self.db_connect(dsn)
        self.dbflag[dsn].acquire()
        conn = self.dbpool[dsn].pop()
        return conn

    def db_query(self, dsn, sql, params=[]):
        conn = self.db_get_from_pool(dsn)
        cur = conn.cursor()
        cur.execute(sql, *params)
        result = cur.fetchall()
        self.db_return_to_pool(dsn, conn)
        return result

    def db_return_to_pool(self, dsn, conn):
        self.dbpool[dsn].append(conn)
        self.dbflag[dsn].release()


    def get_classes(self, feed, option, extra=False):
        classes = self.config[feed].get(self.config[feed].get('settings', 'classify'), option).split(',')
        if extra:
            classes.extend(['skipped', 'failed'])
        return classes

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

    def get_stamp(self):
        return datetime.now().strftime('%Y/%m/%d %H:%M:%S')

    def get_workers(self, feed, section='settings'):
        max_workers = 0
        if 'max_workers' in self.config[feed].options(section):
            max_workers = int(self.config[feed].get(section, 'max_workers'))
        return max_workers

    def get_xpath_check(self, xdoc, xpaths):
        value = None
        if ',' in xpaths:
            for xpath in xpaths.split(','):
                value = self.get_xpath_safe(xdoc, xpath)
                if value:
                    break
        else:
            value = self.get_xpath_safe(xdoc, xpaths)
        return value

    def get_xpath_safe(self, xdoc, xpath):
        try:
            return xdoc.xpath(xpath)[0]
        except:
            return None


    def read_last(self, feed):
        if os.path.isfile('last_'+feed):
            last = open('last_'+feed).read()
        else:
            if 'last_default' in self.config[feed].options('settings'):
                last_default = [self.config[feed].get('settings', 'last_default').split()[:2]]
            else:
                last_default = [self.config[feed].get('settings', 'last_default').split()[:2]]
            last_dict = dict((fmt,float(amt)) for amt,fmt in last_default)
            last = (datetime.now() - timedelta(**last_dict)).strftime(self.config[feed].get('settings', 'last'))
        self.start[feed] = datetime.now().strftime(self.config[feed].get('settings', 'last'))
        return last

    def write_last(self, feed):
        if feed in self.start:
            file = open('last_'+feed, 'w')
            file.write(self.start[feed])
            file.close()


    def pull_file(self, feed, file):
        if self.debug:
            sys.stderr.write('%s [%s] Saving %s\n' % (self.get_stamp(), feed, file))
        with open('%s%s/%s' % (self.tempdir, feed, os.path.basename(file)), 'wb') as handle:
            for block in self.get_site(self.config[feed], stream=file).iter_content(1024):
                if not block:
                    break
                handle.write(block)

    def pull_file_list(self, feed, last):
        cnt = 0
        data = []
        page = 'start'
        while page:
            cnt += 1
            if self.debug:
                sys.stderr.write('%s [%s] Retrieving file list page %d\n' % (self.get_stamp(), feed, cnt))
            if page == 'start':
                data.append(self.get_site(self.config[feed], last).text)
            else:
                data.append(self.get_site(self.config[feed], url=page[0]).text)
            page = re.findall(self.config[feed].get('settings', 'page'), data[-1])
        return ''.join(data)

    def pull_files(self, feed):
        last = self.read_last(feed)
        data = self.pull_file_list(feed, last)
        patt = self.config[feed].get('settings', 'list')
        files = re.findall(patt, data)
        if len(files) == 0:
            sys.stderr.write('%s [%s] No new files to retrieve\n' % (self.get_stamp(), feed))
        else:
            max_workers = self.get_workers(feed)
            if max_workers > 1:
                if self.debug:
                    sys.stderr.write('%s [%s] Retrieving files concurrently with %d workers\n'
                        % (self.get_stamp(), feed, max_workers))
                self.pull_files_concurrent(feed, files, data, max_workers)
            else:
                if self.debug:
                    sys.stderr.write('%s [%s] Retrieving files individually\n' % (self.get_stamp(), feed))
                self.pull_files_single(feed, files, data)

    def pull_files_concurrent(self, feed, files, data, workers):
        with futures.ThreadPoolExecutor(max_workers = workers) as e:
            for file in files:
                e.submit(self.pull_file, feed, file)

    def pull_files_single(self, feed, files, data):
        for file in files:
            self.pull_file(feed, file)


    def sort_file(self, feed, keep, drop, feedpath, file):
        fullpath = feedpath + file
        if not os.path.isfile(fullpath):
            return
        if os.path.getsize(fullpath) == 0:
            os.remove(fullpath)
        else:
            xdoc = etree.parse(fullpath)
            cls = self.get_xpath_check(xdoc, self.config[feed].get(self.config[feed].get('settings', 'classify'), 'xpath'))
            if cls in drop:
                if self.debug:
                    sys.stderr.write('%s [%s] Dropping %s\n' % (self.get_stamp(), feed, file))
                os.remove(fullpath)
            elif cls in keep:
                if self.debug:
                    sys.stderr.write('%s [%s] Classifying %s\n' % (self.get_stamp(), feed, file))
                os.rename(fullpath, feedpath + cls + '/' + file)
            else:
                if self.debug:
                    sys.stderr.write('%s [%s] Skipping %s\n' % (self.get_stamp(), feed, file))
                os.rename(fullpath, feedpath + 'skipped/' + file)

    def sort_files(self, feed):
        keep = self.get_classes(feed, 'keep', extra=True)
        drop = self.get_classes(feed, 'drop')
        feedpath = self.tempdir + feed + '/'
        for cls in keep:
            self.config_path(feedpath + cls)
        files = os.listdir(feedpath)
        if len(files) == 0:
            sys.stderr.write('%s [%s] No files to classify\n' % (self.get_stamp(), feedpath))
        else:
            max_workers = self.get_workers(feed)
            if max_workers > 1:
                if self.debug:
                    sys.stderr.write('%s [%s] Classifying files concurrently with %d workers\n'
                        % (self.get_stamp(), feed, max_workers))
                self.sort_files_concurrent(feed, files, keep, drop, feedpath)
            else:
                if self.debug:
                    sys.stderr.write('%s [%s] Classifying files individually\n' % (self.get_stamp(), feed))
                self.sort_files_single(feed, files, keep, drop, feedpath)

    def sort_files_concurrent(self, feed, files, keep, drop, feedpath):
        with futures.ThreadPoolExecutor(max_workers = self.config[feed].get('settings', 'max_workers')) as e:
            for file in files:
                e.submit(self.sort_file, feed, keep, drop, feedpath, file)

    def sort_files_single(self, feed, files, keep, drop, feedpath):
        for file in files:
            self.sort_file(keep, drop, feedpath, file)


    def parse_class(self, feed, cls, feedpath, with_push=False):
        conf = '%s%s/%s.cf' % (self.conf, self.config[feed].get('settings', 'classify'), cls)
        max_workers = self.get_workers(feed)
        if os.path.isfile(conf):
            self.config_clone(feed)
            self.config[feed].read(conf)
            if max_workers > 1:
                if self.debug:
                    sys.stderr.write('%s [%s] Parsing files concurrently with %d workers\n'
                        % (self.get_stamp(), feed, max_workers))
                if 'merge' in self.config[feed].sections():
                    self.parse_class_latest(feed, self.parse_merges_concurrent(feed, feedpath + cls, max_workers), feedpath + cls)
                self.parse_files_concurrent(feed, feedpath + cls, max_workers, with_push)
            else:
                if self.debug:
                    sys.stderr.write('%s [%s] Parsing files individually\n' % (self,get_stamp(), feed))
                if 'merge' in self.config[feed].sections():
                    self.parse_class_latest(feed, self.parse_merges_single(feed, feedpath + cls), feedpath + cls)
                self.parse_files_single(feed, feedpath + cls, with_push)

    def parse_class_latest(self, feed, filelist, fullpath):
        keep = {}
        for s in filelist:
            if s['group'] not in keep or keep[s['group']]['order'] < s['order']:
                keep[s['group']] = s
        keepers = [ keep[k]['file'] for k in keep ]
        for file in os.listdir(fullpath):
            filepath = '%s/%s' % (fullpath, file)
            if filepath not in keepers:
                if self.debug:
                    sys.stderr.write('%s [%s] Removing outdated %s\n' % (self.get_stamp(), feed, file))
                os.remove(filepath)

    def parse_classes(self, feed, with_push=False):
        self.config[feed].read('%sfeeds/%s.cf' % (self.conf, feed))
        feedpath = self.tempdir + feed + '/'
        classes = self.get_classes(feed, 'keep')
        if len(classes) == 0:
            sys.stderr.write('%s [%s] No classes to parse\n' % (self.get_stamp(), feedpath))
        else:
            max_workers = self.get_workers(feed)
            if max_workers > 1:
                if self.debug:
                    sys.stderr.write('%s [%s] Parsing classes concurrently with %d workers\n'
                        % (self.get_stamp(), feed, max_workers))
                self.parse_classes_concurrent(feed, classes, feedpath, with_push)
            else:
                if self.debug:
                    sys.stderr.write('%s [%s] Parsing classes individually\n' % i(self.get_stamp(), feed))
                self.parse_classes_single(feed, classes, feedpath, with_push)

    def parse_classes_concurrent(self, feed, classes, feedpath, with_push=False):
        with futures.ThreadPoolExecutor(max_workers = len(classes)) as e:
            for cls in classes:
                e.submit(self.parse_class, feed, cls, feedpath, with_push)

    def parse_classes_single(self, feed, classes, feedpath, with_push=False):
        for cls in classes:
            self.parse_class(feed, cls, feedpath, with_push)


    def parse_file(self, feed, fullpath, with_push=False):
        static = {}
        if self.debug:
            sys.stderr.write('%s [%s] Parsing %s\n' % (self.get_stamp(), feed, fullpath))
        xdoc = etree.parse(fullpath)
        static =  self.parse_file_static(feed, fullpath)
        steps = [ s for s in self.config[feed].sections() if re.match('\d',s) ]
        print static

    def parse_file_fail(self, feed, value, file, desc='unknown'):
        if 'fail' in self.config[feed].options('settings'):
            for fail in self.config[feed].get('settings', 'fail').split(','):
                if value and fail in value:
                    if self.debug:
                        sys.stderr.write('%s [%s] Failed %s check: %s\n' % (self.get_stamp(), feed, desc, file))
                    os.rename(file, '%s%s/failed/%s' % (self.tempdir,
                        file.replace(self.tempdir, '').split('/')[0], os.path.basename(file)))

    def parse_file_static(self, feed, fullpath):
        static = {}
        xdoc = etree.parse(fullpath)
        for s in self.config[feed].options('static'):
            static[s] = self.get_xpath_check(xdoc, self.config[feed].get('static', s))
            self.parse_file_fail(feed, static[s], fullpath, 'static')
        return static

    def parse_files_concurrent(self, feed, filepath, workers, with_push=False):
        pool = []
        with futures.ThreadPoolExecutor(max_workers = workers) as e:
            for file in os.listdir(filepath):
                pool.append(e.submit(self.parse_file, feed, filepath + '/' + file, with_push))
            if len(pool) == 0:
                sys.stderr.write('%s [%s] No files to parse\n' % (self.get_stamp(), feedpath))
            pprint ([ p.result() for p in pool ])

    def parse_files_single(self, feed, filepath, with_push=False):
        pool = []
        for file in os.listdir(filepath):
            pool.append(self.parse_file(feed, filepath + '/' + file, with_push))
        pprint (pool)


    def parse_merge(self, feed, fullpath):
        xdoc = etree.parse(fullpath)
        group = self.get_xpath_check(xdoc, self.config[feed].get('merge', 'group'))
        order = self.get_xpath_check(xdoc, self.config[feed].get('merge', 'order'))
        return {'group': group, 'order': order, 'file': fullpath}

    def parse_merges_concurrent(self, feed, filepath, workers):
        pool = []
        with futures.ThreadPoolExecutor(max_workers = workers) as e:
            for file in os.listdir(filepath):
                pool.append(e.submit(self.parse_merge, feed, filepath + '/' + file))
            if len(pool) == 0:
                sys.stderr.write('%s [%s] No files to parse\n' % (self.get_stamp(), feedpath))
            return [ p.result() for p in pool ]

    def parse_merges_single(self, feed, filepath):
        pool = []
        for file in os.listdir(filepath):
            pool.append(self.parse_merge(feed, filepath + '/' + file))
        return pool


    def purge_files(self, feed):
        for subdir in ['skipped', 'failed']:
            fullpath = self.tempdir + feed + '/' + subdir + '/'
            for file in os.listdir(fullpath):
                if self.debug:
                    sys.stderr.write('%s [%s] Deleting %s file: %s\n' % (self.get_stamp(), feed, subdir, file))
                os.remove('%s%s' % (fullpath, file))


    def process_feed(self, feed):
        self.config_feed(feed)
        actions = self.actions.split(',')
        if 'pull' in actions:
            self.pull_files(feed)
        if 'sort' in actions:
            self.sort_files(feed)
        if 'parse' in actions:
            self.parse_classes(feed, with_push='push' in actions)
        if 'push' in actions:
            self.push_tables(feed, with_parse='parse' in actions)
        if 'purge' in actions:
            self.purge_files(feed)
        if 'pull' in actions:
            self.write_last(feed)

    def process_feeds(self):
        feeds = self.feeds.split(',')
        if len(feeds) == 0:
            sys.stderr.write('%s No feeds to process\n' % self.get_stamp())
        else:
            max_workers = self.get_workers('base')
            if max_workers > 1:
                if self.debug:
                    max_workers = len(feeds)
                    sys.stderr.write('%s Processing feeds concurrently with %d workers\n' % (self.get_stamp(), max_workers))
                self.process_feeds_concurrent(feeds)
            else:
                if self.debug:
                    sys.stderr.write('%s Processing feeds individually\n' % (self.get_stamp()))
                self.process_feeds_single(feeds)

    def process_feeds_concurrent(self, feeds):
        with futures.ThreadPoolExecutor(max_workers = len(feeds)) as e:
            for feed in feeds:
                e.submit(self.process_feed, feed)

    def process_feeds_single(self, feeds):
        for feed in feeds:
            self.process_feed(feed)


if __name__ == "__main__":
    tool = Ingestor(sys.argv)
    tool.process_feeds()
