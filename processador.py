from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format, count, regexp_replace

spark = SparkSession.builder.appName("DiarioDeBordo").master("local[*]").getOrCreate()

def processar_arquivo(caminho_csv: str):


    dataframe = spark.read.option("header", "true").option("sep", ";").csv(caminho_csv)

    dataframe = limpar_arquivo(dataframe)

    dataframe_saida = dataframe.select(
        date_format(to_timestamp(col("DATA_INICIO"), "MM-dd-yyyy HH:mm"),"yyyy-MM-dd").alias("DT_REFE"))

    dataframe_saida = dataframe_saida.groupBy("DT_REFE").agg(count("*").alias("QT_CORR")).orderBy("QT_CORR", ascending=False)

    dataframe_saida = dataframe_saida.groupBY("PROPOSITO").agg(count(""))

    print(dataframe.show())
    print(dataframe_saida.show())


def limpar_arquivo(dataframe):
    ## Lida com datas com horas sem um 0 no inicio ex: 1:48 ao invés de 01:48
    dataframe = dataframe.withColumn("DATA_INICIO",
         regexp_replace(col("DATA_INICIO"), r"(\d{2}-\d{2}-\d{4}\s)(\d):", "$10$2:"))

    return dataframe

