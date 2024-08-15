import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, col, array_contains

# Set the S3 bucket and folder paths
parser = argparse.ArgumentParser()
parser.add_argument(
    '--data_source', help='The S3 URI of the data source.')
parser.add_argument(
    '--output_path', help="The S3 URI of the output path.")
args = parser.parse_args()

# Create a SparkSession
spark = SparkSession.builder.appName('The Twitter Grab Corpus').getOrCreate()

# Read the JSONL files into a DataFrame
#tweets_spark_df = spark.read.json(args.data_source) # RevA parameters
tweets_spark_df = spark.read.option('recursiveFileLookup', 'true').json(args.data_source) # RevB parameters

# Define the list of hashtags for DataFrame filtering
hashtags = [
    'sustainability', 
    'Sustainability', 
    'SUSTAINABILITY', 
    'sustainable', 
    'Sustainable', 
    'SUSTAINABLE'
]

expressions = [
    'sustainability', 
    'sustainable'
]

# Create a filtered DataFrame
filtered_tweets_spark_df = tweets_spark_df.filter(
    array_contains('entities.hashtags.text', hashtags[0]) |\
    array_contains('entities.hashtags.text', hashtags[1]) |\
    array_contains('entities.hashtags.text', hashtags[2]) |\
    array_contains('entities.hashtags.text', hashtags[3]) |\
    array_contains('entities.hashtags.text', hashtags[4]) |\
    array_contains('entities.hashtags.text', hashtags[5]) |\
    lower(col('text')).contains(expressions[0]) |\
    lower(col('text')).contains(expressions[1])
)

# Export the DataFrame to JSONL format
filtered_tweets_spark_df.write.mode('overwrite').json(args.output_path)
