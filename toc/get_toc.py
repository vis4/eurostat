import sys
import requests

lang = 'en'
if len(sys.argv) > 1:
   lang = sys.argv[1]

url = 'http://epp.eurostat.ec.europa.eu/NavTree_prod/everybody/BulkDownloadListing?sort=-3&file=table_of_contents_%s.txt' % lang

f = open('toc-%s.txt' % lang, 'w')
f.write(requests.get(url).text.encode('utf-8'))
f.close()
