#SCRAPE INFORMATION ON BOOKS FROM GOODREADS.COM USING ISBN13 CODES
import pandas as pd
import numpy as np
import os
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.signalmanager import dispatcher
from scrapy import signals

#----------START OF MAIN SETTINGS----------#
#repPath = "C:/Users/Andrew/OneDrive - Data ScienceTech Institute/Documents/DSTI/4) Machine Learning with Python Labs/Project/Project 1 - CHOSEN/"
repPath = "/Users/awieber/Documents/Etudes/DSTI/Python Project - GoodReads/Scraping/"
filename = 'ISBNs_to_scrape.tsv'
isbn_import = pd.read_csv(repPath + filename, dtype='str', sep = '\t', index_col = 0)

full_scrape = True     #Boolean to say if doing the full scrape or a partial scrape
ScrapeOn_ISBN13 = False  #Otherwise the scraping is done on isbn10
if full_scrape:
    #Import a modified TSV file containing an extra column showing the replacement characters:
    first_part_filename= "GoodReads_scraped_data_FULL"
    if ScrapeOn_ISBN13:
        fileNameScrappedData = first_part_filename + "_on_ISBN13.tsv"
    else:
        fileNameScrappedData = first_part_filename + "_on_ISBN10.tsv"
else:
    #Search in a subset (testing/finding missing values)
    isbn_import = isbn_import.iloc[:10,:]
    first_part_filename= "GoodReads_scraped_data_PARTIAL"
    if ScrapeOn_ISBN13:
        fileNameScrappedData = first_part_filename + "_on_ISBN13.tsv"
    else:
        fileNameScrappedData = first_part_filename + "_on_ISBN10.tsv"
#----------END OF MAIN SETTINGS----------#

#Generate the list of webpages to scrape
#list_isbn_urls = [('0439785960','https://www.goodreads.com/search?q=0439785960&ref=nav_sb_noss_l_10')]
#page = "https://www.goodreads.com/search?q=" + "0439785960" + "&ref=nav_sb_noss_l_10"
addresstext1 = "https://www.goodreads.com/search?q="
addresstext2 = "&ref=nav_sb_noss_l_10"
isbn_list = (isbn_import[['isbn13','isbn']]).values.tolist()
if ScrapeOn_ISBN13:
    list_isbn_urls_all = [(isbn13_item, isbn_item, addresstext1 + isbn13_item + addresstext2) for isbn13_item, isbn_item in isbn_list]
else:
    list_isbn_urls_all = [(isbn13_item, isbn_item, addresstext1 + isbn_item + addresstext2) for isbn13_item, isbn_item in isbn_list]

#A copy of the full list is used, allowing a subset to specified if needed (especially for testing)
list_isbn_urls = list_isbn_urls_all #list_isbn_urls_all[:10]

#Scrapy object to collect information from the GoodReads website (information missing in initial data)
class GoodReadsCrawler(scrapy.Spider):
    #Scrapy nees a defined name:
    name = 'goodreadscrawler'
    #Set a delay between requests
    custom_settings = {
        'DOWNLOAD_DELAY': 0.15,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'CONCURRENT_REQUESTS': 4
    }
    
    def __init__(self, start_urls=None, *args, **kwargs):
        super(GoodReadsCrawler, self).__init__(*args, **kwargs)
        self.start_urls = start_urls
    
    def start_requests(self):
        for isbn13, isbn, url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, cb_kwargs={'isbn13':isbn13, 'isbn':isbn})

    #Function to extract the genres and the format of each book and link it with the isbn13 and isbn10
    def parse(self, response, isbn13, isbn):
        genres = response.css(".BookPageMetadataSection__genreButton").css(".Button__labelItem::text")
        format = response.xpath('//div[@class="FeaturedDetails"]').xpath('p[@data-testid="pagesFormat"]/text()')
        yield {'isbn13': isbn13, 'isbn': isbn, 'format': format.get(), 'genre': genres.getall()}

#Collect the scrapy information with a signals extension
output = []

def get_output(item):
    output.append(item)

dispatcher.connect(get_output, signal = signals.item_scraped)

process = CrawlerProcess()
process.crawl(GoodReadsCrawler, start_urls=list_isbn_urls)
process.start()

#Export the scraped data
#If file already exists, don't re-export it
if not os.path.exists(repPath + fileNameScrappedData):
    dftoExport = pd.DataFrame(output)
    dftoExport.to_csv(repPath + fileNameScrappedData, sep = "\t")
    print("File exported to ", repPath, fileNameScrappedData, sep = '')
else:
    print("File already exists.")