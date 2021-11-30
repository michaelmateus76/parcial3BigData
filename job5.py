import boto3
import csv
import ntpath
import time
from bs4 import BeautifulSoup

destinationBucket= "parcial3punto1"
def scrapping(fullPath,fileName,newspaper,soup,s3):

    csvFile = open('/tmp/'+fileName+'.csv', 'w',encoding='utf-8')
    writer = csv.writer(csvFile,dialect='unix')
    if(newspaper=="El_tiempo"):
        articles=soup.find_all('article')
        for article in articles:
            category_anchor=article.find("a",{'class':'category'})
            title_anchor= article.find("a",{'class':'title'})
            if(category_anchor and title_anchor):
                category=category_anchor.getText()
                title=title_anchor.getText()
                if("," in title):
                    title=title.replace(",","")
                url='https://www.eltiempo.com'+title_anchor.get('href')
                row=[title,category,url]
                writer.writerow(row)
    elif(newspaper=="El_espectador"):
        articles=soup.findAll('div',{'class':'Card-Container'})
        for article in articles:
            category_div=article.find("h4",{'class':'Card-Section'})
            title_div= article.find("h2",{'class':'Card-Title'})
            if(category_div and title_div):
                category_anchor = category_div.find("a")
                category=category_anchor.getText()
                title_anchor = title_div.find("a")
                title=title_anchor.getText()
                url='https://www.elespectador.com'+title_anchor.get('href')
                row=[title,category,url]
                writer.writerow(row)
    elif(newspaper=='Publimetro'):
        mainDiv=soup.find(id='main')
        divNews=mainDiv.find_all('div',{'class':'container layout-section'})
        usefulDivNews=divNews[1]
        for row in usefulDivNews:
            headers=row.find_all('h4')
            if(len(headers)!=0):  
                for header in headers:
                    section = header.text
                    nextElement=header.nextSibling
                    if nextElement:
                        if(header.parent.name=='main'):
                            items = nextElement.find_all('div',{'class':'list-item'})
                            for news in items:
                                anchor=news.find_all('a')[0]
                                url = "https://www.publimetro.co"+anchor.get('href')
                                title= anchor.get('title')
                                if("," in title):
                                    title=title.replace(",","")
                                row=[title,section,url]
                                writer.writerow(row)
                        else:
                            articles=nextElement.find_all('article')
                            if articles:
                                for article in articles:
                                    headlines = article.find_all('h2')
                                    for line in headlines:
                                        anchor=line.find('a')
                                        if(anchor):
                                            ref = anchor.get('href')
                                            url=""
                                            if (not ref.startswith('https://')):
                                                url = "https://www.publimetro.co"+anchor.get('href')
                                            else:
                                                url = anchor.get('href')
                                            title = anchor.getText()
                                            if("," in title):
                                                title=title.replace(",","")
                                            row=[title,section,url]
                                            writer.writerow(row)
    csvFile.close()
    underFolders = fullPath.replace('headlines/raw','')
    s3.meta.client.upload_file('/tmp/'+fileName+'.csv', destinationBucket,'news/final'+underFolders+'.csv')


if __name__=="__main__":
    bucketName= "parcial3punto1"
    base='headlines/raw/newspaper='
    localtime=time.localtime()
    newspapers=[base + name+ '/year='+str(localtime.tm_year)+'/month='+str(localtime.tm_mon)+'/day='+str(localtime.tm_mday)+'/page.html' for name in ['El_tiempo','Publimetro']]
    for fullName in newspapers:
        fileName=fullName
        s3 = boto3.resource('s3')
        justFileName=ntpath.basename(fileName)   
        s3.meta.client.download_file(bucketName, fileName, '/tmp/'+justFileName)
        f = open('/tmp/'+justFileName,'r',encoding='utf-8')
        txt=f.read()
        soup = BeautifulSoup(txt,'html.parser')
        if("El_tiempo" in fileName):
            scrapping(fileName,justFileName,"El_tiempo",soup,s3)
        elif("El_espectador" in fileName):
            scrapping(fileName,justFileName,"El_espectador",soup,s3)
        elif("Publimetro" in fileName):
            scrapping(fileName,justFileName,"Publimetro",soup,s3)