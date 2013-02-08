import requests
import cloud
import pymongo
from pymongo import MongoClient
from pymongo import Connection
import pickle
import re
#import pdf_to_text

def pdf_to_text(data): 
  from pdfminer.pdfinterp import PDFResourceManager, process_pdf 
  from pdfminer.pdfdevice import PDFDevice 
  from pdfminer.converter import TextConverter 
  from pdfminer.layout import LAParams 

  import StringIO 
  fp = StringIO.StringIO() 
  fp.write(data) 
  fp.seek(0) 
  outfp = StringIO.StringIO() 
  
  rsrcmgr = PDFResourceManager() 
  device = TextConverter(rsrcmgr, outfp, laparams=LAParams()) 
  process_pdf(rsrcmgr, device, fp) 
  device.close() 
  
  t = outfp.getvalue() 
  outfp.close() 
  fp.close() 

def crawler(url, depth):
  connection = MongoClient('23.22.225.88', 27017)
  db = connection.the_real_crawl
  collection = db.test_collection
  try:
    page = requests.get(url)
    page_text = page.text
    try:
      if page.headers['content-type'] == 'application/pdf':
        pdf_as_text = pdf_to_text(page_text)
        db.posts.insert({'content-type': 'application/pdf', 
                         'urls': url, 'pages': pdf_as_text})
    except:
      db.posts.insert({'urls': url, 'pages': page_text})
  except:
    page_text =''
  if (depth == 10):
    return False
  links = re.findall(r'href=[\"\'](http.[^\"\']+)[\"\']',page_text)
  for link in links:
    crawler(link, depth + 1)
        
  
job_id  = cloud.call(crawler, 'http://www.nytimes.com', 0)
job_id2 = cloud.call(crawler, 'http://www.cnn.com.com', 0)
job_id3 = cloud.call(crawler, 'http://www.ycombinator.com', 0)
job_id4 = cloud.call(crawler, 'http://nlp.stanford.edu/IR-book/', 0)

