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
            .withColumn("lat_g", F.regexp_replace("lat", ",", ".").cast('float'))
            .withColumn("lng_g", F.regexp_replace("lng", ",", ".").cast('float'))
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
                  .withColumnRenamed("lat", "lat_e")  # переименование lat на lat_e
                  .withColumnRenamed("lon", "lon_e")  # переименование lon на lon_e
                  .persist()
                  )
    return events_transform_df

# Test
#vents_transform_df = events_transform("/user/master/data/geo/events", spark)
#vents_transform_df.show()

def events_with_geo(events_transform_df: DataFrame, geo_transform_df: DataFrame) -> DataFrame:
    events_with_geo_df = (
        events_transform_df
        # при помощи кросс джоин происходит объединение двух датафреймов "events_df" и "geo_df"
        .crossJoin(geo_transform_df)
        # вычисление расстояния между координатами событий и географическими координатами и именуеься столбцом "distance"
        .withColumn("distance", F.lit(2) * F.lit(6371) * F.asin(
        F.sqrt(
            F.pow(F.sin((F.col("lat_e") - F.col("lat_g"))/F.lit(2)),2)
            + F.cos(F.col("lat_g"))*F.cos(F.col("lat_e"))*
            F.pow(F.sin((F.col("lon_e") - F.col("lng_g"))/F.lit(2)),2)
        )))
        .drop("lat_e","lon_e", "lat_g", "lng_g"))
    # при помощи оконной функции происходит выбор ближайщего географического объекта для каждого события, основываясь на минемальном расстоянии
    window = Window().partitionBy("message_id").orderBy(F.col("distance").asc())
    events_with_geo_df = (
        events_with_geo_df
        .withColumn("row_number", F.row_number().over(window))
        .filter(F.col("row_number")==1)
        .drop("row_number", "distance")
        # добавляется уникальный идентификатор "event_id" для каждого события
        .withColumn("event_id", F.monotonically_increasing_id())
        # выбор и переименование столбцов для удобства дальнейшей обработки
        .selectExpr("message_id", "message_from as user_id", "event_id", "event_type", "id as zone_id", "city", "date")
        .persist()
        )
    
    return events_with_geo_df

# Test
#events_with_geo_df = events_with_geo(events_transform_df, geo_transform_df)
#events_with_geo_df.show()

def travel_calc(events_with_geo_df: DataFrame) -> DataFrame:
    # Группировка данных по "user_id", считая количество путешествий "travel_count" и список городов "travel_array", которые посетил пользователь
    travel_calc_df = (
        events_with_geo_df
        .groupBy("user_id")
        .agg(
            F.count("*").alias("travel_count"),
            F.collect_list("city").alias("travel_array")
        )
    )

    return travel_calc_df

# Test
#travel_calc_df = travel_calc(events_with_geo_df)
#travel_calc_df.show()

def actual_geo(events_with_geo_df: DataFrame) -> DataFrame:
    # при помощи оконной функции происходит группировка строй по "user_id" и упорядочивает их по "data" в порядке убывания
    window = Window().partitionBy("user_id").orderBy(F.col("date").desc())
    actual_geo_df = (events_with_geo_df
            # добавление нового столбца "row_number", который присваивает каждой строке номер в пределах окна
            .withColumn("row_number", F.row_number().over(window))
            # оставляем строки "row_number" только с номером 1, то есть последние события для каждого пользователя
            .filter(F.col("row_number") == 1)
            .selectExpr("message_id", "user_id", "city", "zone_id")   # убрал "travel_count", "travel_array"
            .persist()
           )
    return actual_geo_df

# Test
#actual_geo_df = actual_geo(events_with_geo_df)
#actual_geo_df.show()

def travel_geo(events_with_geo_df: DataFrame) -> DataFrame:
    # при омощи оконной функции группируеи строки "user_id" и "message_id", упорядочивая их по "data"
    window = Window().partitionBy("user_id", "message_id").orderBy(F.col("date"))
    travel_geo_df = (
        events_with_geo_df
        # добавляем новый столбец "dense_rank", который присваивает каждой строке ранг в пределах окна
        .withColumn("dense_rank", F.dense_rank().over(window))
        # добавляем новый столбец "date_diff", который вычисляет разницу в днях между датой события и преобразованным значением "dense_rank"
        .withColumn("date_diff", F.datediff(
            F.col('date').cast(DateType()), F.to_date(F.col("dense_rank").cast("string"), "d")
            )
        )
        .selectExpr("date_diff", "user_id as user", "date", "message_id", "zone_id")
        # группируем строки по "user", "date_diff", "zone_id", посчитывая количество событий "cnt_city" для каждой группы
        .groupBy("user", "date_diff", "zone_id")
        .agg(F.count(F.col("date")).alias("cnt_city"))
        .persist()
        )
    return travel_geo_df

# Test
#travel_geo_df = travel_geo(events_with_geo_df)
#travel_geo_df.orderBy(F.col("cnt_city").desc()).show()

def home_geo(travel_geo_df: DataFrame) -> DataFrame:
    home_city_df = (
        travel_geo_df
        # оставляем строки, где "cnt_city" больше 27
        .filter((F.col("cnt_city") > 27))
        # добавляем новый столбец "max_dt", который содержит максимальное значение "date_diff" для каждого пользователя   
        .withColumn("max_dt", F.max(F.col("date_diff")).over(Window().partitionBy("user"))) 
        # оставляем только те строки, где "data_diff" равен "max_dt"
        .filter(F.col("date_diff") == F.col("max_dt"))       
        .persist()
    )
    return home_city_df

# Test
#home_geo_df = home_geo(travel_geo_df)
#home_geo_df.show()

def merge_actual_and_home_geo(actual_geo_df: DataFrame, home_geo_df: DataFrame, geo_transform_df: DataFrame) -> DataFrame:
    geo_transform_df = geo_transform_df.select("id", "city")
    home_geo_df = (
        # объединяем "home_geo_df" с "geo_transform_df" по "zone_id" и "id", что бы получить название домашнего города "home_city". Затем выбираем столбцы "user" и "home_city"
        home_geo_df
        .join(geo_transform_df, home_geo_df.zone_id == geo_transform_df.id, "inner")
        .selectExpr("user", "city as home_city")
    )

    merge_actual_and_home_geo_df = (
    # объединяем "actual_geo_df" c "home_geo_df" по "user_id" и "user" соотвественно с использованем полного внешнего соединения. Затем выбираем нужные столбцы и переименовываем
       actual_geo_df
       .join(home_geo_df, actual_geo_df.user_id == home_geo_df.user, "fullouter")
       .selectExpr("user_id", "city as act_city", "home_city")    # убрал поля "travel_count", "travel_array"
       .persist()
    )
    return merge_actual_and_home_geo_df

# Test
#merge_actual_and_home_geo_df = merge_actual_and_home_geo_df(actual_geo_df, home_geo_df, geo_transform_df)
#merge_actual_and_home_geo_df.show()

def mart_users_cities(events_path: DataFrame, merge_actual_and_home_geo_df: DataFrame, travel_calc: DataFrame, sql) -> DataFrame:
    times = (
        # загружаем данные из паркет файла, фильтруем собятия типа "message" и выбираем нужные поля
        sql.read.parquet(f'{events_path}')
        .where('event_type = "message"')
        .selectExpr("event.message_from as user_id", "event.datetime", "event.message_id")
        .where("datetime IS NOT NULL")
    )
    # при помощи оконной функции группируем строки по "user_id" и упорядочиваем их по "datetime" в порядке убывания
    window = Window().partitionBy('user_id').orderBy(F.col('datetime').desc())

    times_w = (times
            # добавляем столбец "row_number", который присваивает каждой строки номер в пределах окна
            .withColumn("row_number", F.row_number().over(window))
            # оставляем только те строки с "row_number" равным 1 (последнее событие для каждого пользователя)
            .filter(F.col("row_number")==1)
            # преобразуем поле "datetime" в тип "Timestamp" и отбираем столбцы "user" и "Time"
            .withColumn("TIME", F.col("datetime").cast("Timestamp"))
            .selectExpr("user_id as user", "Time")
    )

    mart_users_cities_df = (
        # объединяем "merge_actual_and_home_geo_df" с "times_w" по "user_id" и "user" соотвественно
        merge_actual_and_home_geo_df
        .join(times_w, merge_actual_and_home_geo_df.user_id == times_w.user, "left").drop("user")
        # добавляем часовой пояс "timezone" при помощи объединения "Australia/" с названием фактического города "act_city"
        .withColumn("timezone",F.concat(F.lit("Australia/"),F.col("act_city")))
        # столбец с текущей меткой времени
        .withColumn("processed_dttm", F.current_timestamp())
        # добавляем столбец "local_time", который вычисляет локальное время. 
	    # Так как функция "from_utc_timestamp" содержит ограниченный список городов, вычисления местного времени будут проводиться не для всех городов.
        .withColumn("local_time", 
                    F.when( F.col("act_city")
                    .isin("Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide", "Canberra", "Hobart", "Darwin"), 
                    F.from_utc_timestamp(
                    F.col("processed_dttm"), 
                    F.concat(F.lit("Australia/"), 
                    F.col("act_city"))))
                    .otherwise(None))
                    .join(travel_calc, merge_actual_and_home_geo_df.user_id == travel_calc.user_id, "left")  # добавил соединение "travel_calc"
        .select(merge_actual_and_home_geo_df["user_id"], "act_city", "home_city", "travel_count", "travel_array", "local_time")
        .persist()
        )
    
    return mart_users_cities_df

# Test
#mart_users_cities_df = mart_users_cities("/user/master/data/geo/events", merge_actual_and_home_geo_df, spark)
#mart_users_cities_df.orderBy(F.col("home_city").desc()).show()

def main() -> None:
    events_path = sys.argv[1]
    geo_path = sys.argv[2]
    output_path = sys.argv[3]
    #events_path = "/user/master/data/geo/events/"
    #geo_path = "/user/denis19/data/geo/cities/actual/geo.csv"
    #output_path = "/user/denis19/analytics/showcase_by_users"

    conf = (SparkConf()
        .setAppName("showcase_recommendations_to_friends")
        .set("spark.executor.memory", "4g")
        .set("spark.driver.memory", "4g"))
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    geo_transform_df = geo_transform(geo_path, sql)
    events_transform_df = events_transform(events_path, sql)
    events_with_geo_df = events_with_geo(events_transform_df, geo_transform_df)
    travel_calc_df = travel_calc(events_with_geo_df)
    actual_geo_df = actual_geo(events_with_geo_df)
    travel_geo_df = travel_geo(events_with_geo_df)
    home_geo_df = home_geo(travel_geo_df)
    merge_actual_and_home_geo_df = merge_actual_and_home_geo(actual_geo_df, home_geo_df, geo_transform_df)
    mart_users_cities_df = mart_users_cities(events_path, merge_actual_and_home_geo_df, travel_calc_df, sql)
    write = mart_users_cities_df.write.mode("overwrite").parquet(f'{output_path}')

    return write

if __name__ == "__main__":
        main()