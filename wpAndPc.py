import BLS_Request
import os
import pyarrow.parquet as pq

path = str(os.path.dirname(os.path.realpath(__file__))) 

def checkForLatestVersion():
    print("Choose an option:")
    print("1: pc        (Industry)")
    print("2: wp        (Commodity)")
    wpORpc = str(input("Type either pc or wp: "))
    BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion(wpORpc)

def readParquet(fileName):
    tempDF = pq.read_table(fileName).to_pandas()
    print(tempDF)
    #tempName = fileName[:-8] + ".csv"
    #tempDF.to_csv(tempName,sep=',',mode='w',line_terminator='\n',header=False,index=False,encoding='utf-8')

def formatTimePeriod():
    print("format time period")

def dropM13():
    print("drop M13")

def labels():
    

#checkForLatestVersion()
print("Choose an option:")
print("1: pc        (Industry)")
print("2: wp        (Commodity)")
wpORpc = str(input("Type either pc or wp: "))
newPath = path + '\\RawData\\' + BLS_Request.getLatestVersionFileName(wpORpc,BLS_Request.getAllFilesInDirectory(wpORpc))
#print(BLS_Request.checkForIndustryOrCommodity(wpORpc, newPath))
print("NEW PATH: " + str(newPath))
readParquet(newPath)