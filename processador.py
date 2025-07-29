from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, date_format, count, regexp_replace, when, sum as sum_, max as max_, min as min_, avg

spark = SparkSession.builder.appName("DiarioDeBordo").master("local[*]").getOrCreate()

def processar_arquivo(caminho_csv: str):
    try:
        dataframe = spark.read.option("header", "true").option("sep", ";").csv(caminho_csv)

        dataframe = limpar_arquivo(dataframe)

        dataframe = dataframe.withColumn(
            "DT_REFE",
            date_format(
                to_timestamp(col("DATA_INICIO"), "MM-dd-yyyy HH:mm"),
                "yyyy-MM-dd"
            )
        )

        dataframe_saida = dataframe.groupBy("DT_REFE").agg(
            count("*").alias("QT_CORR"),
            sum_(when(col("CATEGORIA") == "Negocio", 1).otherwise(0)).alias("QT_CORR_NEG"),
            sum_(when(col("CATEGORIA") == "Pessoal", 1). otherwise(0)).alias("QT_CORR_PESS"),
            max_(col("DISTANCIA").cast("int")).alias("VL_MAX_DIST"),
            min_(col("DISTANCIA").cast("int")).alias("VL_MIN_DIST"),
            avg(col("DISTANCIA").cast("int")).alias("VL_AVG_DIST"),
            sum_(when(col("PROPOSITO") == "Reunião", 1).otherwise(0)).alias("QT_CORR_REUNI"),
            sum_(when(col("PROPOSITO") != "Reunião", 1).otherwise(0)).alias("QT_CORR_NAO_REUNI")

        )

        return dataframe_saida

    except Exception as e:
        print(f"Erro ao processar {caminho_csv}: {e}")


def limpar_arquivo(dataframe):
    ## Lida com datas com horas sem um 0 no inicio ex: 1:48 ao invés de 01:48
    dataframe = dataframe.withColumn("DATA_INICIO",
         regexp_replace(col("DATA_INICIO"), r"(\d{2}-\d{2}-\d{4}\s)(\d):", "$10$2:"))

    return dataframe

