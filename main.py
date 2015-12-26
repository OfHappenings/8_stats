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
        # utcnow() returns local time
        now = datetime.datetime.utcnow()
        page = requests.get(self.catalog_url % (board))

        if page.status_code != 200:
            sys.exit(page.status_code)

        data = page.json()
        
        for page in data:
            # threads, page
            #[u'sticky', u'replies', u'images', u'id', u'no', u'fsize', u'filename', u'tim', u'omitted_images', u'omitted_posts', u'extra_files', u'tn_h', u'last_modified', u'md5', 
            # u'cyclical', u'locked', u'name', u'tn_w', u'h', u'ext', u'resto', u'w', u'time', u'com']

            for line in page['threads']:
                line['timestamp'] = now
                line['board'] = board
    
                exists =  self.es.search(index="8chan", doc_type='catalog', size=1, q='_type:catalog AND board:pol AND id:%s' % (line['id']))

                print(exists['hits']['hits'])

                if exists['hits']['hits']:
                    print(exists)
                    hits = exists['hits']['hits']

                    # fucking stupid
                    if len(hits):
                        old_id = hits[0]['_id']
                        result = self.es.index(index="8chan", id=old_id, doc_type='catalog', body=line)
                        print(result)

                    else:
                        continue


                else:
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






