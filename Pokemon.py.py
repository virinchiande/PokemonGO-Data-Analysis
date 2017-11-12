from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import split,udf,col,monotonically_increasing_id
import  os,math,re,array, sys
from datetime import  datetime
import random

'''Assigning points to clusters based on Euclidean distance'''
def Kmeans(x):
    closest = float("inf")
    j=1
    la=x[1]
    lo=x[0]
    for i in centroidlist:
        dist=math.sqrt(math.pow((la-i[0]),2)+math.pow((lo-i[1]),2))
        if dist<closest:
            closest=dist
            cluster=j
        j+=1
    return [cluster, [la,lo,x[4]]]

'''Computing the centroids'''
def computecentroid(line):
    x=line[0]
    y=list(line[1])
    latsum=0.0
    longsum=0.0
    count=0.0
    for i in y:
        count+=1
        latsum=latsum+i[0]
        longsum=longsum+i[1]
    newx=latsum/count
    newy=longsum/count
    return [x,[newx,newy]]


def timeinhours(t):
    tArray = re.split(":", t)
    minutes = float(tArray[1])
    hour = float(tArray[0])
    if minutes != 0.0:
        tim = minutes / 60.0 + hour
    else:
        tim = hour
    return tim

'''Creating 12 time slots in a day'''
def timeslot(f):
    time24 = datetime.strptime(f,'%H:%M:%S').strftime('%X')
    tim=int(time24[0:2])/2
    timeZone=tim+1
    return timeZone

def PredictNaiveBayes(timezone,weekday):
    count = 0
    idlist = []
    for zone in range (80):
        zoneValue = zone+1
        zon = sqlContext.sql("SELECT Zone FROM bayesDataTable WHERE Zone='" + str(zoneValue) + "'").collect()
        priorprobZone = float(len(zon)) / len(timelist)
        timely = sqlContext.sql(
            "SELECT Time_Zone FROM bayesDataTable WHERE Zone='" + str(zoneValue) + "' AND Time_Zone='" + timezone + "'").collect()
        timelikelyhood = (float(len(timely)) + alpha) / float(len(zon) + alpha * timevocab)
        wkdy = sqlContext.sql(
            "SELECT Weekday FROM bayesDataTable WHERE Zone='" + str(zoneValue) + "' AND Weekday='" + weekday+ "'").collect()
        weekdaylikelyhood = (float(len(wkdy)) + alpha) / float(len(zon) + alpha * weekdayvocab)
        total = priorprobZone * timelikelyhood * weekdaylikelyhood
        classfinal[zoneValue] = total
    keys = max(classfinal.values())
    temp = [x for x, y in classfinal.items() if y == keys]

    # count += 1
    # idlist.append(count)
    # classid = zip(idlist, classification)
    return temp



if __name__ == "__main__":
    sc=SparkContext()
    sqlContext=SQLContext(sc)
    inputFIle = sys.argv[1]
    giventimeZone = sys.argv[2]
    givenWeekday = sys.argv[3]
    ''''file:///home/cloudera/Downloads/green_tripdata_2016-06.csv'''
    df = sqlContext.read.load(inputFIle, format='com.databricks.spark.csv', header='true', inferSchema='true')
    df = df.filter(df.appearedLocalTime.isNotNull())
    alpha=5.0
    '''------------------------------------------Data PreProcessing--------------------------------------------------------------------------------'''

    '''Splitting the column "datetime " into two columns "PickupTime" and "PickupWeekday"'''
    split_col = split(df['appearedLocalTime'], 'T')
	
    '''Adding those two columns to the dataframe'''
    df = df.withColumn('PickupTime', split_col.getItem(1)).withColumn('PickupWeekday', split_col.getItem(0))
    sp=udf(lambda x:datetime.strptime(x,'%Y-%M-%d').strftime('%A'))
    timeUDF = udf(timeinhours)
    timeZone = udf(timeslot)
    totaldata = df.withColumn('Weekday', sp(col('PickupWeekday'))).withColumn("TimeInHours", timeUDF(col('PickupTime'))).withColumn("id",monotonically_increasing_id()).withColumn("Time_Zone", timeZone(col("PickupTime")))
    kmeansDF=totaldata.select('longitude','latitude','TimeInHours','Weekday','id', 'Time_Zone')
    kmeansDF.registerTempTable('bayesdata')
    '''-------------------------------------kmeans---------------------------------------------'''
    ycentroid=sqlContext.sql("SELECT longitude from bayesdata").collect()
    xcentroid=sqlContext.sql("SELECT latitude from bayesdata").collect()
    # xcen=random.sample(xcentroid,4)
    # ycen=random.sample(ycentroid,4)
    xcen=xcentroid[:80]
    ycen=ycentroid[:80]
    centroids=zip(xcen,ycen)
    centroidlist=[]
    for c in centroids:
        temp=[]
        xx=c[0].latitude
        yy=c[1].longitude
        temp=[xx,yy]
        centroidlist.append(temp)

    for itr in range(30):
        counter=0
        ff=kmeansDF.rdd.map(lambda x:Kmeans(x))
        new=ff.groupByKey().map(computecentroid)
        dd=new.collect()
        for itt in dd:
            index=itt[0]
            val=itt[1]
            centroidlist[index-1][0]=val[0]
            centroidlist[index-1][1]=val[1]

    classrow=ff.map(lambda x: (Row(Zone=x[0],ids=x[1][2])))
    finaldf=sqlContext.createDataFrame(classrow)
    naivedf=kmeansDF.join(finaldf,(kmeansDF.id==finaldf.ids))
    '''------------------------------------------------------Naive Bayes Distributed--------------------------------------------'''
    classification = []
    classfinal = {}

    naivedf.registerTempTable('bayesDataTable')
    sqlContext.cacheTable("bayesDataTable")

    timevocab=sqlContext.sql("SELECT count(distinct(Time_Zone)) from bayesDataTable").collect()[0][0]
    weekdayvocab=sqlContext.sql("SELECT count(distinct(Weekday)) from bayesDataTable").collect()[0][0]
    # zoneList = sqlContext.sql("SELECT distinct(Zone) from bayesDataTable").collect()
    min=0.0
    timelist=sqlContext.sql("SELECT Time_Zone FROM bayesDataTable").collect()
    weeklist=sqlContext.sql("SELECT Weekday FROM bayesDataTable").collect()

    classpredicted=PredictNaiveBayes(giventimeZone, givenWeekday)
    print "Area with highest demand given time and day is ",classpredicted
    print "Top three highest demand areas are ", sorted(classfinal,key=classfinal.get,reverse=True)[:3]
