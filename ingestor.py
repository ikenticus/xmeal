'''
Universal Sports Ingestor Script
Author: ikenticus
Created: 2013/11/11
'''

import os
import re
import sys
import base64
import requests
import ConfigParser
from lxml import etree
from datetime import datetime, timedelta


class Ingestor:
    def __init__(self, args):
        self.cmd = os.path.basename(args[0])

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
            for s in self.config.options('global'):
                setattr(self, s, self.config.get('global', s))
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


    def get_files(self, feed, data):
        for file in self.get_list(data, self.config.get('settings', 'list')):
            sys.stderr.write('Saving %s...\n' % file)
            with open('%s%s/%s' % (self.tempdir, feed, os.path.basename(file)), 'wb') as handle:
                for block in self.get_site(self.config, stream=file).iter_content(1024):
                    if not block:
                        break
                    handle.write(block)


    def retrieve_files(self, feed):
        self.config.read('%sfeeds/%s.cf' % (self.conf, feed))
        if not os.path.isdir('%s/%s' % (self.tempdir, feed)):
            os.makedirs('%s/%s' % (self.tempdir, feed))
        if os.path.isfile('last_'+feed):
            last = open('last_'+feed)
        else:
            last = (datetime.now() - timedelta(hours=1)).strftime(self.config.get('settings', 'last'))
        self.get_files(feed, self.get_site(self.config, last).text)


    def bucket_files(self, feed):
        feedpath = self.tempdir + feed + '/'
        self.config.read('%sfeeds/%s.cf' % (self.conf, feed))

        classes = self.config.get('settings', self.config.get('settings', 'classify')).split(',')
        classes.append('skipped')
        for c in classes:
            if not os.path.isdir(feedpath + c):
                os.makedirs(feedpath + c) 

        for file in os.listdir(feedpath):
            fullpath = feedpath + file
            if not os.path.isfile(fullpath):
                continue
            if os.path.getsize(fullpath) == 0:
                os.remove(fullpath)
            else:
                root = etree.parse(fullpath)
                category = root.xpath(self.config.get('settings', 'xpath'))[0]
                sys.stderr.write('Categorizing %s...\n' % file)
                if category in classes:
                    os.rename(fullpath, feedpath + category + '/' + file)
                else:
                    os.rename(fullpath, feedpath + 'skipped/' + file)
                    

    def process_feed(self, feed):
        #self.retrieve_files(feed)
        self.bucket_files(feed)


    def process(self):
        for feed in self.feeds.split(','):
            self.process_feed(feed)


if __name__ == "__main__":
    tool = Ingestor(sys.argv)
    tool.process()
