import requests
from bs4 import BeautifulSoup


url = 'http://epp.eurostat.ec.europa.eu/NavTree_prod/everybody/BulkDownloadListing?dir=data&sort=-3&sort=2&start=all'

if __name__ == '__main__':
   r = requests.get(url)
   table = 
