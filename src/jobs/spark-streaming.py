from time import sleep
import logging
import openai
import pyspark 
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, FloatType
from pyspark.sql.functions import col, from_json, udf , when
from config.config import config 


def sentiment_analysis(comment) -> str:

    """
    Performs sentiment analysis using OpenAI GPT-3.5-turbo model.

    Parameters:
    - comment (str): The input comment for sentiment analysis.

    Returns:
    - str: The predicted sentiment (POSITIVE, NEGATIVE, NEUTRAL).
    """

    if comment:
        openai.api_key = config["openai"]["api_key"]
        completion = openai.ChatCompletion.create(
            model='gpt-3.5-turbo',
            messages = [
                {
                    "role":"system",
                    "content": """
                    You're a machine learning model with a task of classifying comments into POSITIVE, NEGATIVE, NEUTRAL.
                    You are to respond with one word from the option specified above, do not add anything else.
                    Here is  the comment:
                    {comment}
                    """.format(comment=comment)
                }
            ]
        )
        return completion.choices[0].message['content']
    return "Empty"

def start_streaming(spark):

    """
    Starts Spark Streaming to consume data from a socket and perform sentiment analysis.

    Parameters:
    - spark (SparkSession): The Spark session.

    Note: This function continuously retries in case of exceptions, with a 10-second delay.
    """

    topic = 'customers_review'
    while True:
        try:
            # Read streaming data from a socket
            stream_df = (spark.readStream.format("socket")
                        .option("host","spark-master")
                        .option("port", 9999)
                        .load())
            
            # Define schema for the streaming data
            schema = StructType([
                StructField("review_id",StringType()),
                StructField("user_id",StringType()),
                StructField("business_id",StringType()),
                StructField("stars",FloatType()),
                StructField("date",StringType()),
                StructField("text",StringType())
            ])

            # Apply schema to the streaming data
            stream_df = stream_df.select(from_json(col('value'),schema).alias("data")).select(("data.*"))

            # Apply sentiment analysis function as a User-Defined Function (UDF)
            sentiment_analysis_udf = udf(sentiment_analysis, StringType())
            stream_df = stream_df.withColumn('feedback',
                                             when(col('text').isNotNull(), sentiment_analysis_udf(col('text')))
                                             .otherwise(None)
                                             )
            
#query= stream_df.writeStream.outputMode("append").format("console").options(truncate=False).start() ## i used this to print the results in console to check 
#query.awaitTermination()


            # Prepare data for writing to Kafka
            kafka_df=stream_df.selectExpr("CAST(review_id AS STRING) AS key", "to_json(struct(*)) AS value" )

            # Write data to Kafka topic
            query = (kafka_df.writeStream
                .format("kafka")
                .option("kafka.bootstrap.servers", config['kafka']['bootstrap.servers'])
                .option("kafka.security.protocol", config['kafka']['security.protocol'])
                .option('kafka.sasl.mechanism' , config ['kafka']['sasl.mechanisms'])
                .option('kafka.sasl.jaas.config',
                        'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" '
                        'password="{password}";'.format(
                            username=config['kafka']['sasl.username'],
                            password=config['kafka']['sasl.password']
                        ))
                .option('checkpointLocation', '/tmp/checkpoint')
                .option('topic', topic) #topic is where the data will be stored in the cloud 
                .start()
                .awaitTermination()
                    )
        
        except Exception as e :
            logging.error("Exception encoutered: {e}. Retrying in 10 seconds")
            sleep(10)

    

if __name__ == "__main__":
    spark_conn = SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()
    start_streaming(spark_conn)
