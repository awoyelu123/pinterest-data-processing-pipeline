import credentials
import pyspark
import findspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,IntegerType

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
df = spark.read.json('s3a://pintrest-data-b5d598e8-12fd-488f-8e22-215f5936044/pintrest-data-b5d598e8-12fd-488f-8e22-215f5936044/message10.json')




 #Find out what this means
df = df.replace({'User Info Error': None}, subset = ['follower_count']) \
                     .replace({'No description available Story format': None}, subset = ['description']) \
                     .replace({'No description available': None}, subset = ['description']) \
                     .replace({'Image src error.': None}, subset = ['image_src'])\
                     .replace({'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e': None}, subset = ['tag_list'])\
                     .replace({"No Title Data Available": None}, subset = ['title'])

df = df.withColumn('follower_count', regexp_replace('follower_count', 'k', '000'))\
        .withColumn('follower_count', regexp_replace('follower_count', 'M', '000000'))       

df = df.withColumn("downloaded",col("downloaded").cast(BooleanType()))\
        .withColumn("index",col("index").cast(IntegerType()))\
        .withColumn("follower_count",col("follower_count").cast(IntegerType()))



# reorder columns
df = df.select('unique_id',
                        'index',
                        'title',
                        'category',
                        'description',
                        'follower_count',
                        'tag_list',
                        'is_image_or_video',
                        'image_src',
                        'downloaded',
                        'save_location'
                        )

df.show()
print(df.dtypes)