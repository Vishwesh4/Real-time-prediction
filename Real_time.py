from __future__ import print_function 
from pyspark.context import SparkContext
from pyspark.ml import Pipeline,PipelineModel
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import Row,StringType,IntegerType,DoubleType
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import split,udf,col,regexp_replace,pandas_udf, PandasUDFType
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from collections import Counter
import datetime 
# import pandas as pd
import numpy as np
import datetime 
import pickle as pkl
# import xgboost
import os
import sys
import json
import time 

kafka_topic = 'from-pubsub'
zk = '10.138.0.3:2181'
app_name = 'from-pubsub' # Can be some other name
sc = SparkContext(appName="KafkaPubsub")
ssc = StreamingContext(sc, 30)
sc.setLogLevel("FATAL")

kafkaStream = KafkaUtils.createStream(ssc, zk, app_name, {kafka_topic: 1})


def getSparkSessionInstance(sparkConf):
    if ("sparkSessionSingletonInstance" not in globals()):
        globals()["sparkSessionSingletonInstance"] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()["sparkSessionSingletonInstance"]

#Dictionary of mapping between number and label, we got this from training code
A = {'label': ['NY', 'K', 'Q', 'BX', 'R']}

################################################## UDFs ##############################################
#Finds the label
def valueToCategory(value):
  return A["label"][int(value)]
#Finds the accuracy
def accuracy_calc(value1,value2):
    #Adding this structure to handle unknown labels
    try:
      temp = A["label"][int(value2)]
      if value1==temp:
          return 1
      else:
          return 0
    except:
      return 0
#Finds the day of the week
#Extracting information from date
def day_finder(x):
    return datetime.datetime.strptime(x, '%m/%d/%Y').weekday()
#Bucketizing violation time
def time_bucket(x):
  #Bucketizing the time into 8 buckets
  if x is None:
    return 3
  if x[-1]!='P' and x[-1]!='A':
    return 3
  try:
    time = int(x[:-1])
  except:
    return 3
  if x[-1]=='P':
    time = 1200+time
  for i in range(8):
    if time>=300*i and time<300*(i+1):
      return i 
#Defining XGBOOST prediction UDF
# @f.pandas_udf(returnType=DoubleType())
# def predict_pandas_udf(*cols):
#     # cols will be a tuple of pandas.Series here.
#     X = pd.concat(cols, axis=1)
#     return pd.Series(xgboost_model.predict(np.array(X))) 
udfValueToCategory = udf(valueToCategory, StringType())
udfaccuracy = udf(accuracy_calc,IntegerType())
time_udf = udf(time_bucket,IntegerType())
day_udf = udf(day_finder,IntegerType())
print("############################################## START ################################################################")

lines = kafkaStream.map(lambda x: json.loads(x[1])["spam"]).map(lambda x: x.split(","))
#Loading pipeline
model = PipelineModel.load('gs://ch16b024/model_finalproject_v1/')
#Considered columns
column_names = ['Registration_State_index','Plate_Type_index','Violation_Code_index',
'Vehicle_Body_Type_index','Vehicle_Make_index','Issuing_Agency_index','Street_Code1_index',
'Street_Code2_index','Street_Code3_index','Issuer_Precinct_index','Issuer_Command_index',
'Violation_In_Front_Of_Or_Opposite_index','Violation_County_index','Month','Day','Time']
#Loading xgboost model
# xgboost_model = pkl.load(open("gs://ch16b024/XGB_final_model_v1.pkl", "rb"))
accuracy = 0
completed = 0
def process(rdd):
        start = time.time()
        global accuracy
        global completed
        # Get the singleton instance of SparkSession
        spark = getSparkSessionInstance(rdd.context.getConf())        
        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda x: Row(Summons_Number=str(x[0]),Registration_State=str(x[2]),Plate_Type=str(x[3]),
        Violation_Code=str(x[5]),Vehicle_Body_Type=str(x[6]),Vehicle_Make=str(x[7]),
        Issuing_Agency=str(x[8]),Street_Code1=str(x[9]),Street_Code2=str(x[10]),
        Street_Code3=str(x[11]),Violation_County=str(x[13]),
        Issuer_Precinct=str(x[14]),Issuer_Command=str(x[16]),Issuer_Squad=str(x[17]),
        Violation_In_Front_Of_Or_Opposite=str(x[21]),Issue_Date=str(x[4]),
        Violation_Time=str(x[18]),Violation_Location=str(x[20])))
        df = spark.createDataFrame(rowRdd)
        ############################################## PREPROCESSING ##################################
        #Splitting the issue date into month,year,day
        df_new = df.withColumn('Month',split('Issue_Date','/')[0]).withColumn('Year',split('Issue_Date','/')[2]).withColumn('Day',day_udf(col('Issue_Date'))).withColumn('Time',time_udf(col('Violation_Time')))
        #converting the columns into integers
        df_new = df_new.withColumn("Year",df_new["Year"].cast(IntegerType())).withColumn("Month",df_new["Month"].cast(DoubleType())).withColumn("Day",df_new["Day"].cast(DoubleType())).withColumn("Time",df_new["Time"].cast(DoubleType()))
        #Removing outliers and some filtering
        df_new =df_new.drop(*['Issue_Date','Violation_Time','Year','Issuer_Squad'])
        #Filling na
        df_new = df_new.fillna({'Time':3})
        #Removing na locaions of violation location and violation count
        df_new = df_new.dropna(how='any',subset=['Violation_Location','Violation_County'])
        df_new=df_new.dropna(how='any')
        #Mapping violation location
        df_new = df_new.withColumn('Violation_Location', regexp_replace('Violation_Location', 'KINGS', 'K'))\
        .withColumn('Violation_Location', regexp_replace('Violation_Location', 'KING', 'K'))\
        .withColumn('Violation_Location', regexp_replace('Violation_Location', 'QUEEN', 'Q'))\
        .withColumn('Violation_Location', regexp_replace('Violation_Location', 'QU', 'Q'))\
        .withColumn('Violation_Location', regexp_replace('Violation_Location', 'NEWY', 'NY'))\
        .withColumn('Violation_Location', regexp_replace('Violation_Location', 'NEW Y', 'NY'))\
        .withColumn('Violation_Location', regexp_replace('Violation_Location', 'MAN', 'NY'))\
        .withColumn('Violation_Location', regexp_replace('Violation_Location', 'MH', 'NY'))\
        .withColumn('Violation_Location', regexp_replace('Violation_Location', 'BRONX', 'BX'))
        # df_new.show()
        ################################################################################################
        # Prediction using saved model
        df_r1 = model.transform(df_new)
        # df_r1.show()
        # df_r1.dropna()
        df_with_cat = df_r1.withColumn("correct", udfaccuracy("label","prediction"))
        # df_with_cat.show()
        correct_array = df_with_cat.select("label","prediction").rdd.map(lambda r: int(r[0])-int(r[1])==0).collect()
        num = len(correct_array)
        temp = sum(correct_array)
        completed+=num
        accuracy += temp
        end = time.time()
        print("Labels correct till now:{}/{}".format(accuracy,completed))
        print("Completed batch of {} in {}sec".format(num,end-start))
        df_r1.show()
        
lines.foreachRDD(process)


ssc.start() # Start the computation
ssc.awaitTermination() # Wait for the computation to terminate