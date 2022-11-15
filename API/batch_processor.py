import credentials
import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf


findspark.init()
bucketname = 'pintrest-data-b5d598e8-12fd-488f-8e22-215f5936044'


# Creating our Spark configuration
conf = SparkConf() \
    .setAppName('S3toSpark') \
    .setMaster('local[*]')


sc=SparkContext(conf=conf)

# Configure the setting to read from the S3 bucket
accessKeyId= credentials.AWSAccessKeyId
secretAccessKey= credentials.AWSSecretKey


hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', accessKeyId)
hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem") 
hadoopConf.set("fs.s3a.endpoint", "s3.amazonaws.com")
# Create our Spark session
spark=SparkSession(sc)


# Read from the S3 bucket
df = spark.read.json('s3a://pintrest-data-b5d598e8-12fd-488f-8e22-215f5936044/pintrest-data-b5d598e8-12fd-488f-8e22-215f5936044/message0.json')
df.show()

