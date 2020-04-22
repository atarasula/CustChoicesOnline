from lxml import html
import configparser
import requests
import base64
import string
import sys
from datetime import datetime
from collections import OrderedDict
import teradatasql
import pandas as pd
import numpy as np
import os

currdate = datetime.now().strftime('%Y-%m-%d')
curr_hour = datetime.now().strftime('%H')

print("Started Pogram @ " + datetime.now().strftime("%d-%b-%Y (%H:%M:%S)"))

# THIS SCRIPTS EXPECTS 1 PARAMETER - bannerID
args = sys.argv
if len(args) < 2:
    print("Error! Excepting Parameter Banner_ID (HXN,NYC,FTF)")
    sys.exit(1)

#############OPEN Config.ini file and read all vars
path_current_directory = os.path.dirname(__file__)
path_config_file = os.path.join(path_current_directory, 'Config.ini')
config = configparser.ConfigParser()
config.read(path_config_file)

homePath = config.get('PATHS', 'HOME_PATH')
logPath = os.path.join(homePath, 'Logs\\') 
passPath = config.get('PATHS', 'ENCRYPT_PASS_PATH')
banner_id = args[1]

# TERADATA DB STUFF
hostServer = config.get('DATABASE', 'HOST')
userID = config.get('DATABASE', 'USERNAME')
userPWD = r'ENCRYPTED_PASSWORD(file:' + passPath + userID + '_PassKey.properties,file:' + passPath + userID + '_EncPass.properties)'
DBName = config.get('DATABASE', 'DB')
#########################

outfile = open(logPath + '\\' + datetime.now().strftime("%Y%m%d%H%M%S") + '_' + banner_id + '_SiteData.log', 'w', buffering=1)

def logMessage(strMessage: object) -> object:
    sOut = datetime.now().strftime(logDTFormat) + ' - ' + strMessage
    outfile.write(sOut + '\n')
    print(sOut)

try:

    # if one of the hosts is down, the other should be up, but hardcoded for now
    eneHost = '129.80.201.37'
    # eneHost='129.80.201.39'
    # use banner_id to determine siteUrl and port for endeca server
    if banner_id == 'NYC':
        siteMapURL = "https://www.nyandcompany.com/sitemap/"
        enePort = '3041'
        Ne = '102216'
        Other = '+4294967259'
    if banner_id == 'FTF':
        siteMapURL = "https://www.fashiontofigure.com/sitemap/"
        enePort = '3046'
        Ne = '102216'
        Other = '+4294967259'
    if banner_id == 'HXN':
        siteMapURL = "https://www.happyxnature.com/sitemap/"
        enePort = '3044'
        Ne = '391840508'
        Other = '+3573931349'

    endecaURL = 'http://atg-workbench-prod-lnyi.oracleoutsourcing.com/endeca_jspref/controller.jsp?displayKey=P_Name&enePort=' + enePort + '&Np=1&Nu=product.masterStyle&Ne=' + Ne + '&eneHost=' + eneHost + '&N=XXXCATEORYIDXXX' + Other

    logDTFormat = "%d-%b-%Y %H:%M:%S"

    logMessage('Opening SiteMap ' + siteMapURL + '..')

    page = requests.get(siteMapURL)
    tree = html.fromstring(page.content)
    catList = tree.xpath('//div[@class="noncatalog-content"]//li/a')

    # get unique URLs to avoid dupes by reassigning to catDict dictionary
    logMessage('Getting unique URLs\Categories..')
    catDict = {}
    for el in catList:
        if not el.get('href') in catDict.keys():
            catDict[el.get('href')] = [el.text]

    logMessage('Total Number of Unique URLs\Categories: ' + str(len(catDict)))

    finalList = []
    countsList = []
    i = 0
    errCount = 0
    logMessage('Opening Each Category Page..')

    with teradatasql.connect(host=hostServer, user=userID, password=userPWD) as connect:
        with connect.cursor() as cur:

            sql = "DELETE " + DBName + ".HR_WEBCLK_CUST_CHOICE WHERE RECORD_INSERT_DATE='" + currdate + "' AND BANNER_ID='" + banner_id + "' AND RECORD_INSERT_HR=" + curr_hour
            cur.execute(sql)
            FinalList = []
            for key, value in catDict.items():

                i += 1

                try:
                    categoryPageUrl = key
                    categoryTitle = value[0]
                    urlChunksList = categoryPageUrl.split("/")
                    # get categories from url
                    categoryList = urlChunksList[3:-2]
                    categoryString = '/'.join(categoryList)
                    # get each category page for using url from site map list
                    outfile.write('\n')
                    print(' ')

                    logMessage('#' + str(i) + ' Opening ' + categoryPageUrl)

                    catPage = requests.get(categoryPageUrl)
                    pageTree = html.fromstring(catPage.content)

                    logMessage('Finding totalNumRecs..')
                    itemCountList = pageTree.xpath('//input[@id="totalNumRecs"]/@value')

                    # some pages have no item count so skip those

                    if len(itemCountList) > 0:
                        itemCount = itemCountList[0]
                        categID = str(urlChunksList[-2]).replace('N-', '')

                        # GET SKU COUNT FROM ENDECA
                        logMessage('Opening Edneca Page..')
                        eUrl=endecaURL.replace('XXXCATEORYIDXXX', categID)
                        logMessage(eUrl)
                        endecaPageResp = requests.get(eUrl)
                        endecaPageTree = html.fromstring(endecaPageResp.content)

                        logMessage('Finding Matching Records:..')

                        skuCountList = endecaPageTree.xpath(
                            "//i[contains(text(), 'Matching Records:')]/parent::font/font/text()")
                        # print(str(skuCountList))
                        styleCountList = endecaPageTree.xpath(
                            "//i[contains(text(), 'Aggregated Records:')]/parent::font/font/text()")

                        # print (str(urlChunksList[-3]) + ": Display Item Count=" +prodList[0]+ " Sku Count=" + str(skuCountList[0]) + " Style Count=" + str(styleCountList[0]))

                        # countsList=[categoryPageUrl,categoryString,element.text,itemCount,styleCountList[0],skuCountList[0]]
                        # finalList.append(countsList)

                        countStr = banner_id + ' ' + datetime.now().strftime(
                            logDTFormat) + ' ' + categoryString + ' Items:' + str(
                            itemCountList[0]) + ' Skus:' + str(skuCountList[0]) + ' Styles:' + str(styleCountList[0])
                        logMessage(countStr)
                        datarow = [banner_id, currdate,curr_hour, categoryPageUrl, categoryString, categoryTitle,
                                   str(itemCountList[0]).replace(',', ''), str(styleCountList[0]).replace(',', ''),
                                   str(skuCountList[0]).replace(',', ''), datetime.now()]
                        FinalList.append(datarow)
                        sql = "INSERT INTO " + DBName + ".HR_WEBCLK_CUST_CHOICE (?,?,?,?,?,?,?,?,?,?)"
                        cur.execute(sql, datarow)

                except:
                    logMessage(str(sys.exc_info()))
                    errCount += 1
                    if errCount > 10:
                       logMessage("Number of errors exceeded threshold, exiting program..")
                       break
                    continue
                finally:
            #just in case there is too many iterations
                    if i > 400:
                        break

    logMessage("Dumping dataset to " + logPath + banner_id + "_SiteData.csv ..")
    header = "process date,category PageUrl,category String,category Title,item Count,style Count,skuCountList,datetime"
    np.savetxt(logPath + '\\' + banner_id + "_SiteData.csv", FinalList, delimiter=",", fmt='%s', header=header)
except:
    logMessage(str(sys.exc_info()))

logMessage("Completed.")

outfile.close()
