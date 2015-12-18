import sys
import argparse
import datetime

import requests
from elasticsearch import Elasticsearch


class Stats():
    def __init__(self):
        self.es          = Elasticsearch()
        self.catalog_url = 'http://8ch.net/%s/catalog.json'
        self.boards_url  = 'http://8ch.net/boards.json'




    def upload_catalog(self, board):
        now = datetime.datetime.now()
        page = requests.get(self.catalog_url % (board))

        if page.status_code != 200:
            sys.exit(page.status_code)

        data = page.json()
        
        for page in data:
            # threads, page
            #[u'sticky', u'replies', u'images', u'id', u'no', u'fsize', u'filename', u'tim', u'omitted_images', u'omitted_posts', u'extra_files', u'tn_h', u'last_modified', u'md5', 
            # u'cyclical', u'locked', u'name', u'tn_w', u'h', u'ext', u'resto', u'w', u'time', u'com']

            for line in page['threads']:
                print(line)
                line['timestamp'] = now
                result = self.es.index(index="8chan", doc_type='catalog', body=line)
                print(result)



if __name__ == "__main__":
    stats = Stats()

    parser = argparse.ArgumentParser(description='send 8chan stats to elasticsearch')
    parser.add_argument('--catalog', action='store')
    parser.add_argument('--boards',  action='store_true')

    args = parser.parse_args()

    if args.catalog:
        stats.upload_catalog(args.catalog)






