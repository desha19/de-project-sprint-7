import sys
import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window 
from pyspark.sql.types import DateType

os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["YARN_CONF_DIR"] = "/etc/hadoop/conf"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
os.environ["HADOOP_CONF_DIR"] = "/etc/hadoop/conf/"

#spark = SparkSession.builder \
#                    .master("yarn") \
#                    .appName("Project_7") \
#                    .getOrCreate()

# функция для чтения файла "/user/denis19/data/geo/cities/actual/geo.csv" и переобразования
def geo_transform(geo_path: str, sql) -> DataFrame:
    geo_transform_df = (sql.read.option("header", True)
            .option("delimiter", ";")
            .csv(geo_path)
            .withColumn('lat_g', F.regexp_replace('lat', ',', '.').cast('float'))
            .withColumn('lng_g', F.regexp_replace('lng', ',', '.').cast('float'))
            .drop("lat", "lng")
            .persist()
            )
    return geo_transform_df

# Test
#geo_transform_df = geo_transform("/user/denis19/data/geo/cities/actual/geo.csv", spark)
#geo_transform_df.show()

# функция чтения "/user/master/data/geo/events" с переименованием стобцов lat на lat_e и lon на lon_e
def events_transform(events_path: str, sql) -> DataFrame:
    events_transform_df = (sql
                  .read.parquet(f'{events_path}')
                  .where('event_type = "message"')
                  .select("event.message_id", "event.message_from","event_type", "lat", "lon", "date")
                  .where('lat IS NOT NULL and lon IS NOT NULL')
                  .withColumnRenamed("lat", "lat_e")  # Переименование lat на lat_e
                  .withColumnRenamed("lon", "lon_e")  # Переименование lon на lon_e
                  .persist()
                  )
    return events_transform_df

# Test
#vents_transform_df = events_transform("/user/master/data/geo/events", spark)
#vents_transform_df.show()

def events_with_geo(events_transform_df: DataFrame, geo_transform_df: DataFrame) -> DataFrame:
    events_with_geo_df = (
        # объединяем датафреймы "events_transform_df" и "geo_transform_df"
        events_transform_df
        .crossJoin(geo_transform_df)
        # добавим столбец "event_id" с уникакльными значениями, используя функцию "monotonically_increasing_id"
        .withColumn('event_id', F.monotonically_increasing_id())
        # добавляем столбец "distance", который вычисляет расстояние между координатами событий и географическими координатами
        .withColumn("distance", F.lit(2) * F.lit(6371) * F.asin(
        F.sqrt(
            F.pow(F.sin((F.col("lat_e") - F.col("lat_g"))/F.lit(2)),2)
            + F.cos(F.col("lat_g"))*F.cos(F.col("lat_e"))*
            F.pow(F.sin((F.col("lon_e") - F.col("lng_g"))/F.lit(2)),2)
        )))
        # удаляем не нужные столбцы
        .drop("lat_e","lon_e", "lat_g", "lng_g"))
    # создадим окно, которое группирует строки по "messenge_id" и сортирует их по возростанию расстояния
    window = Window().partitionBy('event_id').orderBy(F.col('distance').asc())
    events_with_geo_df = (
        events_with_geo_df
        # добавим столбец "row_number", который присваивает каждой строке уникальный номер в пределах группы
        .withColumn("row_number", F.row_number().over(window))
        # оставляем только строки с минимальным значением "distance" для каждого "message_id"
        .filter(F.col('row_number')==1)
        # удаление временных полей
        .drop('row_number', 'distance')
        # выбираем и переименовываем необходимые столбцы
        .selectExpr("message_id", "message_from as user_id", "event_id", "event_type", "id as zone_id", "city", "date")
        .persist()
        )
    return events_with_geo_df

# Test
#events_with_geo_df = events_with_geo(events_transform_df, geo_transform_df)
#events_with_geo_df.show()

def mart_zones(events_with_geo_df: DataFrame) -> DataFrame:
    # группируем данные по "user_id" и сортируем их по дате по возрастанию
    window = Window().partitionBy('user_id').orderBy(F.col('date').asc())
    # группируем данные по "zone_id" и месяцу
    w_month = Window.partitionBy(['zone_id', F.trunc(F.col("date"), "month")])
    # группируем данные по "zone_id" и неделе
    w_week = Window.partitionBy(['zone_id', F.trunc(F.col("date"), "week")])

    registrations_df = (
        events_with_geo_df
        # добавляем столбец "row_number", который присваивает каждой строке уникальный номер в пределах группы "user_id"
        .withColumn("row_number", F.row_number().over(window))
        # фильтруем строки, оставляя только первую строку для каждого "user_id"
        .filter(F.col('row_number')==1)
        .drop('row_number')
        # добавляем столбцы "month" и "week", которые содержат усечённые значения даты до месяцы и недели
        .withColumn("month",F.trunc(F.col("date"), "month"))
        .withColumn("week",F.trunc(F.col("date"), "week"))
        # добавляем столбцы "week_user" и "month_user", которые содержат количество уникальных пользователей за неделю и месяц
        .withColumn("week_user", F.count('user_id').over(w_week))
        .withColumn("month_user", F.count('user_id').over(w_month))
        .selectExpr("month","week", "week_user", "month_user")
        .distinct()
        )

    mart_zones_df = (events_with_geo_df
          # добавляем столбцы "month" и "week" с усечёнными значениями даты
          .withColumn("month",F.trunc(F.col("date"), "month"))
          .withColumn("week",F.trunc(F.col("date"), "week"))
          # добавляем столбцы с суммами за неделю и месяц
          .withColumn("week_message",F.sum(F.when(events_with_geo_df.event_type == "message",1).otherwise(0)).over(w_week))
          .withColumn("week_reaction",F.sum(F.when(events_with_geo_df.event_type == "reaction",1).otherwise(0)).over(w_week))
          .withColumn("week_subscription",F.sum(F.when(events_with_geo_df.event_type == "subscription",1).otherwise(0)).over(w_week))
          .withColumn("month_message",F.sum(F.when(events_with_geo_df.event_type == "message",1).otherwise(0)).over(w_month))
          .withColumn("month_reaction",F.sum(F.when(events_with_geo_df.event_type == "reaction",1).otherwise(0)).over(w_month))
          .withColumn("month_subscription",F.sum(F.when(events_with_geo_df.event_type == "subscription",1).otherwise(0)).over(w_month))
          # объединяем данные с "registrations_df" по столбцам "month" и "week"
          .join(registrations_df, ["month", "week"], "fullouter")
          .select("month", "week", "zone_id", "week_message", "week_reaction", "week_subscription", "week_user", "month_message", "month_reaction", "month_subscription", "month_user")
          .distinct()
          )
    return mart_zones_df

# Test
#mart_zones_df = mart_zones(events_with_geo_df)
#mart_zones_df.show()

def main() -> None:
    events_path = sys.argv[1]
    geo_path = sys.argv[2]
    output_path = sys.argv[3]
    #events_path = "/user/master/data/geo/events/"
    #geo_path = "/user/denis19/data/geo/cities/actual/geo.csv"
    #output_path = "/user/denis19/analytics/showcase_in_section_of_zones"

    conf = (SparkConf()
        .setAppName("project_7_showcase_in_section_of_zones")
        .set("spark.executor.memory", "4g")
        .set("spark.driver.memory", "4g"))
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    geo_transform_df = geo_transform(geo_path, sql)
    events_transform_df = events_transform(events_path, sql)
    events_with_geo_df = events_with_geo(events_transform_df, geo_transform_df)
    mart_zones_df = mart_zones(events_with_geo_df)
    write = mart_zones_df.write.mode('overwrite').parquet(f'{output_path}')

    return write

if __name__ == "__main__":
        main()