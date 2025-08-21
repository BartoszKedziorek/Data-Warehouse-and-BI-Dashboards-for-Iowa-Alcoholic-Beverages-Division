from pyspark.sql import SparkSession


def main():
    
    spark: SparkSession = SparkSession.builder \
        .appName("Iowa Sales Analytic Platform ETL") \
        .setMaster("yarn") \
        .getOrCreate()
    
    df = spark.read.csv("./include/data.csv", header="true")
    df.show()

    df.write("/test/tmp")
    
    # spark.stop()

if __name__ == "__main__":
    main()