import pyspark
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from delta import *

if __name__ == "__main__":
    try:
        builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("fs.azure.account.key.<<storage-name>>.dfs.core.windows.net", "<<secret-key>>")

        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        # temp = spark.read.csv("abfss://<<container>>@<<storage-name>>.dfs.core.windows.net/carro.csv")  
        # 
        # temp.write.format("delta").mode("overwrite").save("abfss://<<container>>@<<storage-name>>.dfs.core.windows.net/carro")
        # print("Sucesso")

        df = spark.read.format("delta").load("abfss://<<container>>@<<storage-name>>.dfs.core.windows.net/carro")
        df.show(10)
    except Exception as e:
        print(e)