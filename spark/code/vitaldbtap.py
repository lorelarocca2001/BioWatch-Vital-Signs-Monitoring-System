from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import from_json, col, round, when
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.ml.pipeline import PipelineModel
import os

# Kafka and Elasticsearch configuration
kafkaServer = os.getenv("KAFKA_SERVER", "broker:9092")
topic = os.getenv("KAFKA_TOPIC", "vitaldb")
modelPath = os.getenv("MODEL_PATH", "/opt/tap/model/riskanalyzer")
elastic_index = os.getenv("ELASTIC_INDEX", "vitalparameters")
elastic_host = os.getenv("ELASTIC_HOST")
elastic_port = os.getenv("ELASTIC_PORT", "9243")
elastic_user = os.getenv("ELASTIC_USER", "elastic")
elastic_password = os.getenv("ELASTIC_PASSWORD")

# Configure Spark with connection to Elasticsearch
sparkConf = SparkConf() \
    .set("es.nodes", elastic_host) \
    .set("es.port", elastic_port) \
    .set("es.net.http.auth.user", elastic_user) \
    .set("es.net.http.auth.pass", elastic_password) \
    .set("es.nodes.wan.only", "true") \
    .set("es.net.ssl", "true")

# Creating the Spark session
spark = SparkSession.builder.appName("vitaldbtap").config(conf=sparkConf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Loading the Machine Learning model
print("Loading model...")
riskAnalyzerModel = PipelineModel.load(modelPath)
print("Model loaded successfully.")

# Defining the schema for Kafka messages
kafka_schema = StructType([
    StructField("caseid", StringType(), True),
    StructField("Age", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("Height", StringType(), True),
    StructField("Weight", StringType(), True),
    StructField("Diastolic Blood Pressure", StringType(), True),
    StructField("Systolic Blood Pressure", StringType(), True),
    StructField("Heart Rate", StringType(), True),
    StructField("Body Temperature", StringType(), True),
    StructField("Respiratory Rate", StringType(), True),
    StructField("Oxygen Saturation", StringType(), True),
    StructField("timestamp", StringType(), True)
])

print("Reading stream from Kafka...")

# Reading the stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", topic) \
    .option("failOnDataLoss", "false") \
    .load()

# Parsing JSON
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", kafka_schema).alias("data")) \
    .select("data.*")

# Calculating derived parameters
df = df.withColumn("Derived_Pulse_Pressure", round(col("Systolic Blood Pressure") - col("Diastolic Blood Pressure"), 2)) \
       .withColumn("Derived_BMI", round(col("Weight") / (col("Height") ** 2), 2)) \
       .withColumn("Derived_MAP", round(col("Diastolic Blood Pressure") + (col("Systolic Blood Pressure") - col("Diastolic Blood Pressure")) / 3, 2))

# List of numerical columns that require type conversion
numeric_columns = ['Heart Rate', 'Respiratory Rate', 'Body Temperature', 'Oxygen Saturation',
                   'Systolic Blood Pressure', 'Diastolic Blood Pressure', 'Age', 'Weight',
                   'Height', 'Derived_Pulse_Pressure', 'Derived_BMI', 'Derived_MAP']

# Cast selected columns to FloatType for consistency
for col_name in numeric_columns:
    df = df.withColumn(col_name, col(col_name).cast(FloatType()))

# Apply the Machine Learning model
df = riskAnalyzerModel.transform(df)

# Interpret the prediction output
df = df.withColumn("Risk Predicted", when(col("prediction") == 1, "Low Risk").otherwise("High Risk"))

# Select relevant output columns
df = df.select("caseid", "Age", "Gender", "Height", "Weight", "Heart Rate", "Respiratory Rate",  
               "Body Temperature", "Oxygen Saturation", "Systolic Blood Pressure",  
               "Diastolic Blood Pressure", "Derived_Pulse_Pressure", "Derived_BMI",  
               "Derived_MAP", "timestamp", "Risk Predicted")

# Write to Elasticsearch Cloud
df.writeStream \
   .outputMode("append") \
   .format("org.elasticsearch.spark.sql") \
   .option("es.resource", elastic_index) \
   .option("es.nodes", elastic_host) \
   .option("es.port", elastic_port) \
   .option("es.nodes.wan.only", "true") \
   .option("es.net.ssl", "true") \
   .option("es.net.http.auth.user", elastic_user) \
   .option("es.net.http.auth.pass", elastic_password) \
   .option("checkpointLocation", "/tmp/") \
   .option("es.mapping.date.rich", "true") \
   .start() \
   .awaitTermination()




