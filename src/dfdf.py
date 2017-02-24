'''
Created on Feb 2, 2017

@author: madhawac
'''
from pyspark.shell import sqlContext
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

if __name__ == '__main__':    
    df = sqlContext.read.parquet('/data/intermediate_data/cdr_step5_1/')
    df_test = sqlContext.read.parquet('/data/intermediate_data/cdr_step5/')
    
    labeled_df = StringIndexer(inputCol='churned', outputCol='label').fit(df).transform(df)
    reduced_numeric_cols = ["coefficiant_of_variance_in", "coefficiant_of_variance_out", "call_count_in", "call_count_out"]
    assembler = VectorAssembler(inputCols=reduced_numeric_cols, outputCol='features')
    assembler.transform(df)
    # (train, test) = df_test.randomSplit([0.4, 0.6])
    classifier = RandomForestClassifier(labelCol='label', featuresCol='features')
    pipeline = Pipeline(stages=[label_indexer, assembler, classifier])
    model = pipeline.fit(df)
    predictions = model.transform(df_test)
    predictions.write.mode("overwrite").saveAsTable("cdr_step6_1", format="parquet", path="/data/intermediate_data/cdr_step6_1/")
    evaluator = BinaryClassificationEvaluator(labelCol="churned", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    precision = evaluator.evaluate(predictions)
    print("Precision= %g" % precision)
    
