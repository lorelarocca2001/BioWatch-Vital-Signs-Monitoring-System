from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import pyspark.sql.types as tp

# Initialize a Spark session
spark = SparkSession.builder.appName("RiskAnalyzer").getOrCreate()
# To reduce verbose output during Spark operations
spark.sparkContext.setLogLevel("ERROR") 

# Define the schema of the dataset 
schema = tp.StructType([
    tp.StructField(name='Patient ID', dataType=tp.IntegerType(), nullable=True),
    tp.StructField(name='Heart Rate', dataType=tp.IntegerType(), nullable=True),
    tp.StructField(name='Respiratory Rate', dataType=tp.IntegerType(), nullable=True),
    tp.StructField(name='Body Temperature', dataType=tp.FloatType(), nullable=True),
    tp.StructField(name='Oxygen Saturation', dataType=tp.FloatType(), nullable=True),
    tp.StructField(name='Systolic Blood Pressure', dataType=tp.IntegerType(), nullable=True),
    tp.StructField(name='Diastolic Blood Pressure', dataType=tp.IntegerType(), nullable=True),
    tp.StructField(name='Age', dataType=tp.IntegerType(), nullable=True),
    tp.StructField(name='Gender', dataType=tp.StringType(), nullable=True), 
    tp.StructField(name='Weight', dataType=tp.FloatType(), nullable=True),
    tp.StructField(name='Height', dataType=tp.FloatType(), nullable=True),
    tp.StructField(name='Derived_BMI', dataType=tp.FloatType(), nullable=True),
    tp.StructField(name='Risk Category', dataType=tp.StringType(), nullable=True)
])

# Read the dataset from CSV with defined schema
print("Reading training set...")
dataset = (
    spark.read.format("csv")
    .option("header", "true")  
    .schema(schema)
    .load("/tmp/dataset/human_vital_signs_dataset.csv")
)
print("Done.")

# List of features to be used in the classification model
classification_features = [
    'Age', 'Heart Rate', 'Respiratory Rate', 'Body Temperature',  'Oxygen Saturation',
    'Systolic Blood Pressure', 'Diastolic Blood Pressure', 'Derived_BMI', 
    ]

# Convert the categorical target 'Risk Category' into numeric labels
risk_indexer = StringIndexer(inputCol="Risk Category", outputCol="Risk_Category_Encoded", handleInvalid="keep")

# Combine selected features into a single vector for the classifier
assembler = VectorAssembler(inputCols=classification_features, outputCol="features")

# Initialize a Random Forest classifier 
rf = RandomForestClassifier(featuresCol="features", labelCol="Risk_Category_Encoded", numTrees=100)

# Create the pipeline
pipeline = Pipeline(stages=[risk_indexer, assembler, rf])

# Split the dataset into training (70%) and test (30%) sets
train_data, test_data = dataset.randomSplit([0.7, 0.3], seed=1234)

# Train the model on the training set
print("Training the model...")
model = pipeline.fit(train_data)
print("Model training completed.")

# Make predictions on the test set
predictions = model.transform(test_data)

# Evaluate accuracy
evaluator_accuracy = MulticlassClassificationEvaluator(labelCol="Risk_Category_Encoded", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator_accuracy.evaluate(predictions)
print(f"Accuracy: {accuracy:.4f}")

# Evaluate precision
evaluator_precision = MulticlassClassificationEvaluator(labelCol="Risk_Category_Encoded", predictionCol="prediction", metricName="weightedPrecision")
precision = evaluator_precision.evaluate(predictions)
print(f"Precision: {precision:.4f}")

# Evaluate recall
evaluator_recall = MulticlassClassificationEvaluator(labelCol="Risk_Category_Encoded", predictionCol="prediction", metricName="weightedRecall")
recall = evaluator_recall.evaluate(predictions)
print(f"Recall: {recall:.4f}")

# Show the top 10 predictions with selected columns
predictions.select("Patient ID", "Risk Category", "Risk_Category_Encoded", "prediction").show(10)

# Extract feature importances
rf_model = model.stages[-1]  # The last stage of the pipeline is the trained RF model
feature_importances = rf_model.featureImportances

# Print feature importances
print("Feature Importances:")
for feature, importance in zip(classification_features, feature_importances):
    print(f"{feature}: {importance:.4f}")

# Save the trained model
print("Saving model...")
model.write().overwrite().save("/opt/tap/model/riskanalyzer")
print("Done.")

# Stop the Spark session to free resources
spark.stop()