# -*- coding: utf-8 -*-
"""
Created on Tue May 12 23:39:12 2020

@author: supde
"""

import camelot
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import os
import shutil
import glob
import textract
import pandas as pd
import ocrmypdf
import re
import fitz

class containment_list_parser:
    INPUT_FOLDER_NAME = "inputPdfs"
    PARSED_CSV_FOLDER_NAME = "parsedCSVs"
    PARSED_TEXT_FOLDER_NAME = "parsedTexts"
    
    LIST_TO_CSV = ['kolkata','howrah','malda','nadia','medinipur','24','dinajpur']
    
    def __init__(self,url):
        self.urlToScrape = url
        self.pdfUrlList=[]
        
    def check_pdf_has_text(self,file_name):
        page_num = 0
        text_perc = 0.0
        doc = fitz.open(file_name)
        for page in doc:
            page_num = page_num + 1
            page_area = abs(page.rect)
            text_area = 0.0
            for b in page.getTextBlocks():
                r = fitz.Rect(b[:4]) # rectangle where block text appears
                text_area = text_area + abs(r)
            text_perc = text_perc + (text_area / page_area)
        text_perc = text_perc / page_num
        # If the percentage of text is very low, the document is most likely a scanned PDF
        if text_perc < 0.01:
            return True
        else:
            return False
            
    def scrape_for_pdfs(self):
        htmlpage = requests.get(self.urlToScrape)
        if(htmlpage.status_code == 200):
            print("Successfully fetched page")
        else:
            print("Page fetch failed!")
            raise Exception("Failed")
        soup = BeautifulSoup(htmlpage.content, 'html.parser')
        for fafapdf in soup.find_all(class_="fa-file-pdf-o"):
            if fafapdf.parent.name == 'a':
                #print(fafapdf.parent)
                if fafapdf.parent.has_attr('href'):
                    #print(fafapdf.parent['href'])
                    self.pdfUrlList.append(urljoin(self.urlToScrape, '/'+fafapdf.parent['href']))
                    
        print("PDF file urls got: ",len(self.pdfUrlList))
    
    def freshDir(self, foldername):
        try:
            shutil.rmtree(foldername)
        except OSError as e:
            print ("Failed to delete. Folder not present: "+ e.filename)
        try:
            os.mkdir(foldername)
        except OSError as e:
            print ("Creation of the directory failed: "+ e.filename)
            raise Exception("Failed")
        else:
            print ("Successfully created the directory")
    
    def downloadPdfs(self):
        self.freshDir(self.INPUT_FOLDER_NAME)
        print("Downloading...")
        for url in self.pdfUrlList:
            r = requests.get(url, allow_redirects=True)
            open(self.INPUT_FOLDER_NAME+'/'+url.split('/')[-1], 'wb').write(r.content)
            print("Downloaded "+ url.split('/')[-1])
          
    def combineTablesToSingle(self, tables):
        dfAll = pd.DataFrame()
        for table in tables:
            #print(table.df)
            dfAll = dfAll.append(table.df, ignore_index = True)
        #print(dfAll)
        return dfAll
        
        
    def extractPDFToCSV(self,path_to_pdf):
        regionName = path_to_pdf.split('.')[-2].split('\\')[-1]
        print("Reading PDF: "+path_to_pdf+ "  ....")
        tables = camelot.read_pdf(path_to_pdf,pages="1-end")
        filename = self.PARSED_CSV_FOLDER_NAME+"/"+regionName+'.csv'
        dataframeAll = self.combineTablesToSingle(tables)
        #print(dataframeAll)
        dataframeAll.to_csv(filename)
    
    def extractPDFToText(self,path_to_pdf):
        regionName = path_to_pdf.split('.')[-2].split('\\')[-1]
        filename = self.PARSED_TEXT_FOLDER_NAME+"/"+regionName+".txt"
        textFileObjWrite = open(filename, 'wb')
        extractedText = textract.process(path_to_pdf)
        textFileObjWrite.write(extractedText)
        textFileObjWrite.close()
        
        
    
    def processPDFs(self):
        self.freshDir(self.PARSED_TEXT_FOLDER_NAME)
        self.freshDir(self.PARSED_CSV_FOLDER_NAME)
        for file in glob.glob("./"+self.INPUT_FOLDER_NAME+"/*.pdf"):
            if any(substring in file.lower() for substring in self.LIST_TO_CSV):
                print("Processing to csv: "+file+ "  ....")
                self.extractPDFToCSV(file)
            else:
                print("Processing to text: "+file+ "  ....")
                self.extractPDFToText(file)
                
    def convertNonOCRToOCR(self):
        for file in glob.glob("./"+self.INPUT_FOLDER_NAME+"/*.pdf"):
            if self.check_pdf_has_text(file):
                print("Converting to OCR: "+file)
                ocrmypdf.ocr(file, file, deskew=True, use_threads=True)
    
    def getListOfAddresses(self,filename):
        MIN_SIZE = 3
        areaName = filename.split('.')[-2].split('\\')[-1]
        with open(filename, 'r') as fileObj:
            allText = fileObj.read()
            allText = re.sub('[^A-Za-z0-9 \n.,]+', ' ', allText)
            listAllText = allText.split('\n')
            listAllText = [item for item in listAllText if len (item) > MIN_SIZE]
            #print(listAllText)
            listFilteredText = []
            iterator = 0
            while iterator <len(listAllText):
                if 'containment ' in listAllText[iterator].lower():
                    if iterator < len(listAllText) - 1:
                        nextItem = listAllText[iterator + 1]
                    else:
                        nextItem = ''
                    listFilteredText.append(listAllText[iterator] + ' '+ nextItem+', '+areaName)
                    iterator += 1
                elif 'ward ' in listAllText[iterator].lower():
                    listFilteredText.append(listAllText[iterator]+', '+areaName)
                elif bool(re.search("[0-9]{1,3}.+,.+,",listAllText[iterator])):
                    listFilteredText.append(listAllText[iterator]+', '+areaName)
                iterator += 1
                
            return listFilteredText
    
    def convertTextsToCSV(self):
        for file in glob.glob("./"+self.PARSED_TEXT_FOLDER_NAME+"/*.txt"):
            print("Converting text to csv: "+file)
            addrList = self.getListOfAddresses(file)
            dfAddrList = pd.DataFrame({'Area':addrList})
            areaName = file.split('.')[-2].split('\\')[-1]
            outFilename = self.PARSED_CSV_FOLDER_NAME+"/"+areaName+'.csv'
            dfAddrList.to_csv(outFilename)
    
    def performFlow(self):
        self.scrape_for_pdfs()
        self.downloadPdfs()
        self.convertNonOCRToOCR()
        self.processPDFs()
        self.convertTextsToCSV()
        #waitfor manual checking
        
        
if __name__ == '__main__':
    ccp = containment_list_parser("https://wb.gov.in/containment-zones-in-west-bengal.aspx")
    ccp.performFlow()
