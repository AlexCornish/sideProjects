import requests
import datetime
import json
import prettytable
import pandas as pd
import csv
import urllib3
import re
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import os
import time

path = str(os.path.dirname(os.path.realpath(__file__))) 

def checkForLatestVersion(wpOrpc):
    # currentLastestVersion: The information about the currentLatestVersion that will be compared against the time and date of the latest version available on the BLS website. 
    # wpOrpc: Indicates whether the data to be accessed is from wp (commodity) or pc (industry).
    # Gets the main downloads page from which the time of latest update can be accessed
    URL = "https://download.bls.gov/pub/time.series/{}/".format(wpOrpc)
    # The URL is selected
    page = requests.get(URL)
    tempString = str(page.text)
    tempString = tempString.split()
    latestDate = ""
    for i in range(1,len(tempString)):
        if "Current" in tempString[i]:
            for j in range(i-5, i-2):
                latestDate += tempString[j] + " "
    return convertToDateObj(latestDate)

def compareLatestOnlineVersionWithLatestDownloadedVersion(wpOrpc):
    downloadDate, downloadTime = determineLatestVersionDownloaded(getAllFilesInDirectory(wpOrpc))
    fileName = checkForLatestVersion(wpOrpc).split("_")
    newVerDate = datetime.date(int(fileName[0]),int(fileName[1]),int(fileName[2]))
    newVerTime = datetime.time(int(fileName[3]),int(fileName[4]))
    if newVerDate == downloadDate and newVerTime == downloadTime:
        print("Latest version is already downloaded.")
    elif newVerDate > downloadDate:
        print("Downloading latest version")
        t0 = time.time()
        getBLSData("https://download.bls.gov/pub/time.series/pc/pc.data.0.Current",wpOrpc)
        t1 = time.time()
        totalTime = t1 - t0
        print("total time: " + str(totalTime))

def checkForIndustryOrCommodity(wpOrpc, newPath):   
    currentPath = ""
    if wpOrpc == "pc":
        currentPath = newPath + '\\Industry'
    elif wpOrpc == "wp":
        currentPath = newPath + '\\Commodity'
    if not os.path.exists(currentPath):
        os.makedirs(currentPath)
        return currentPath
    else:
        return currentPath
        
def getAllFilesInDirectory(wpOrpc):
    filesInDirectory = []
    newPath = path + '\\RawData'
    if not os.path.exists(newPath):
        os.makedirs(newPath)
    currentPath = checkForIndustryOrCommodity(wpOrpc,newPath)
    for file in os.listdir(currentPath):
        if file.endswith(".parquet"): 
            if wpOrpc == "pc" and "industry" in file:
                filesInDirectory.append(file)
            elif wpOrpc == "wp" and "commodity" in file:
                filesInDirectory.append(file)
    return filesInDirectory

def determineLatestVersionDownloaded(filesInDirectory):
    latestTime = datetime.time()
    latestDate = datetime.date.fromtimestamp(0)
    for fileName in filesInDirectory:
        date, time = extractTimeFromFileName(fileName)
        if date > latestDate:
            latestDate = date
            latestTime = time
    return latestDate, latestTime

def extractTimeFromFileName(fileName):
    fileName = fileName[:-8].split("_")
    extractedDate = datetime.date(int(fileName[2]),int(fileName[3]),int(fileName[4]))
    extractedTime = datetime.time(int(fileName[5]),int(fileName[6]))
    return extractedDate, extractedTime
    
def getLatestVersionFileName(wpOrpc,filesInDirectory):
    latestTime = datetime.time()
    latestName = ""
    latestDate = datetime.date.fromtimestamp(0)
    for fileName in filesInDirectory:
        date, time = extractTimeFromFileName(fileName)
        if date > latestDate:
            latestDate = date
            latestName = fileName
            latestTime = time
    if wpOrpc == "pc":
        return "Industry\\" + fileName
    else:
        return "Commodity\\" + fileName



def pmConverter(dateTimeStr):
    dateTimeStr = datetime.datetime.strptime(dateTimeStr[:-4], '%m/%d/%Y %H:%M')
    dateTimeStr = dateTimeStr.replace(hour=dateTimeStr.hour+12)
    return dateTimeStr

def convertToDateObj(dateTimeStr):
        if "PM" in dateTimeStr:
            return convertFormat(str(pmConverter(dateTimeStr))[:-3])
        timeStr = str(datetime.datetime.strptime(dateTimeStr[:-4], '%m/%d/%Y %H:%M'))[:-3]
        return convertFormat(timeStr)

def convertFormat(dateTimeStr):
    dateTimeStr = dateTimeStr.replace(" ","_").replace(":","_").replace("-","_")
    return dateTimeStr

def convertRawDataTOPyArrowFormat(rawData, wpOrpc):
    tempName = path + '\\RawData'
    fileName = createFileName(checkForLatestVersion(wpOrpc),wpOrpc) + ".parquet"
    tempName = checkForIndustryOrCommodity(wpOrpc,tempName) + "\\" + fileName
    df = pd.DataFrame(rawData)
    table = pa.Table.from_pandas(df)
    pq.write_table(table,tempName)

def getBLSData(url, wpOrpc):
    http = urllib3.PoolManager()
    r = http.request('GET',url)
    tempInfo = r.data.decode("utf-8")
    tempArr = []
    tempInfo = tempInfo.splitlines()
    for j in tempInfo:
        row = re.split(r'\t+',j)
        for k in range(0,len(row)):
            row[k] = row[k].strip()
        tempArr.append(row)
    convertRawDataTOPyArrowFormat(tempArr,wpOrpc)

def createFileName(latestVersionDate,wpOrpc):
    # wp (commodity) and pc (industry)
    if wpOrpc == "pc":
        return "industry_data_" + latestVersionDate
    else:
        return "commodity_data_" + latestVersionDate
