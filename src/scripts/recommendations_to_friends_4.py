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

def geo_transform(geo_path: str, sql) -> DataFrame:
    cities_geo = (sql.read.option("header", True)
            .option("delimiter", ";")
            .csv(geo_path)
            .withColumn("lat_g", F.regexp_replace("lat", ",", ".").cast("float"))
            .withColumn("lng_g", F.regexp_replace("lng", ",", ".").cast('float'))
            .drop("lat", "lng")
            .persist()
            )
    return cities_geo

# Test
#geo_transform_df = geo_transform("/user/denis19/data/geo/cities/actual/geo.csv", spark)
#geo_transform_df.show()

def events_transform_from(events_path: str, sql) -> DataFrame:
    events_transform_from = (sql
        .read.parquet(events_path)
        # отобрать только те строки, где "event_type" = "message"
        .where('event_type = "message"')
        # отбираем необходимые столбцы
        .selectExpr("event.message_id as message_id_from", "event.message_from", "event.subscription_channel", "lat", "lon", "date")
        # отбираем только те строки, где нет NULL значений
        .where("lat IS NOT NULL and lon IS NOT NULL")
        # переименовываем столбцы для удобства
        .withColumnRenamed("lat", "lat_eff")  
        .withColumnRenamed("lon", "lon_eff")
        # отбираем только те строки, где нет NULL значений  
        .where("message_from IS NOT NULL")
        .persist()
    )
    
    window = Window().partitionBy("message_from").orderBy(F.col("date").desc())
    events_transform_from = (
        events_transform_from
        # добавляем новый столбец "row_number", который содержит номер строки в каждой группе, отсортированной по дате
        .withColumn("row_number", F.row_number().over(window))
        # фильтруем строки, оставляя только первую строку в каждой группе (самую последнюю по дате)
        .filter(F.col("row_number") == 1)
        .drop("row_number")
        .persist()
    )

    return events_transform_from

# Test
#events_transform_from_df = events_filtered_from("/user/master/data/geo/events", spark)
#events_transform_from_df.orderBy(F.col("subscription_channel").desc()).show(n=10)

def events_subscriptions(events_path: str, sql) -> DataFrame:
    # чтение паркет файла и переименовываем столбц "subscription_channel" на "ch"
    events_subscription = (sql
        .read.parquet(events_path)
        .selectExpr("event.user as user", "event.subscription_channel as ch") 
        .where("user is not null and ch is not null")
        # группируем строки по столбцу "user". Для каждой группы собираем все значения "ch" в список и сохраняем столбец "chans"
        .groupBy("user").agg(F.collect_list(F.col("ch")).alias("chans"))
        .persist()
    )
    
    return events_subscription

# Test
#events_subscriptions_df = events_subscriptions("/user/master/data/geo/events", spark)
#events_subscriptions_df.show()

def events_union_sender_receiver(events_path: str, sql) -> DataFrame:
    # производим чтение паркет файла выбираеи и переименовываем отобранные столбцы "message_from" на "sender", "message_to" на "reciever"
    sender_receiver_df = (sql
        .read.parquet(events_path)
        .selectExpr("event.message_from as sender", "event.message_to as reciever") 
        .where("sender is not null and reciever is not null")
    )
    # производим чтение паркет файла выбираеи и переименовываем отобранные столбцы "message_from" на "sender", "message_to" на "reciever"
    receiver_sender_df = (sql
        .read.parquet(events_path)
        .selectExpr("event.message_to as reciever", "event.message_from as sender") 
        .where("sender is not null and reciever is not null")
    )
    # проводим объединение "sender_receiver_df" и "receiver_sender_df" и удаляем дубликаты 
    events_union_sender_receiver_df = (sender_receiver_df
        .union(receiver_sender_df)
        .distinct()
    )
    # добавляем новый столбец "sender_reciever_existing", который содержит строку, объединяющую значения "sender" и "reciever"
    events_union_sender_receiver_df = (events_union_sender_receiver_df
        .withColumn("sender_reciever_existing", F.concat(events_union_sender_receiver_df.sender, F.lit("-"), events_union_sender_receiver_df.reciever))
        # удаляем не нужные столбцы
        .drop("sender", "reciever")
    )
    
    return events_union_sender_receiver_df

# Test
#events_union_sender_receiver_df = events_union_sender_receiver("/user/master/data/geo/events", spark)
#events_union_sender_receiver_df.show()

def recommendations(events_transform_from: DataFrame, geo_transform: DataFrame, events_subscription: DataFrame, events_union_sender_receiver: DataFrame) -> DataFrame:
    result = (
        events_transform_from.alias("from1")
            .crossJoin(events_transform_from.alias("from2"))
            # вычисляем растояние между координатами "lat_eff", "lon_eff" и "lat_eff", "lon_eff"
            .withColumn("distance", F.lit(2) * F.lit(6371) * F.asin(
                F.sqrt(
                F.pow(F.sin((F.col('from1.lat_eff') - F.col('from2.lat_eff')) / F.lit(2)),2)
                + F.cos(F.col("from1.lat_eff"))*F.cos(F.col("from2.lat_eff")) *
                F.pow(F.sin((F.col('from1.lon_eff') - F.col('from2.lon_eft')) / F.lit(2)),2)
        )))
        # фильтруем строки, где расстояние меньше или равно 1
        .where("distance <= 1")
        # вычисляем среднюю точку между координатами "lat_eff", "lon_eff" и "lat_eff", "lon_eff"
        .withColumn("middle_point_lat", (F.col('from1.lat_eff') + F.col('from2.lat_eff'))/F.lit(2))
        .withColumn("middle_point_lon", (F.col('from1.lon_eff') + F.col('from2.lon_eff'))/F.lit(2))
        # выбираем и переименовываем столбцы "message_id_from" на "user_left", "message_id_to" на "user_right" и удаляем дубликаты
        .selectExpr("from1.message_id_from as user_left", "from2.message_id_to as user_right", "middle_point_lat", "middle_point_lon")
        .distinct()
        .persist()
        )

    result = (
        result
        # выполняем крос джоин с дата фрейм "geo_transform"
        .crossJoin(geo_transform)
        # вычисляем расстояние между сркдней точкой и координатами из "geo_transform"
        .withColumn("distance", F.lit(2) * F.lit(6371) * F.asin(
        F.sqrt(
            F.pow(F.sin((F.col('middle_point_lat') - F.col('lat_g'))/F.lit(2)),2)
            + F.cos(F.col("middle_point_lat"))*F.cos(F.col("lat_g"))*
            F.pow(F.sin((F.col('middle_point_lon') - F.col('lng_g'))/F.lit(2)),2)
        )))
        .select("user_left", "user_right", "id", "city", "distance")
        .persist()
        )
    
    window = Window().partitionBy("user_left", "user_right").orderBy(F.col('distance').asc())
    result = (
        result
            .withColumn("row_number", F.row_number().over(window))
            .filter(F.col('row_number') == 1)
            .drop('row_number', "distance", "id")
            # добавляем столбец "timezone", объединяя "Australia/" с "city"
            .withColumn("timezone",F.concat(F.lit("Australia/"),F.col("city")))
            # переименовываем столбец
            .withColumnRenamed("city", "zone_id")
            # добавляем столбец "sender_reciever_all" объединяя "user_left" и "-" и "user_right"
            .withColumn('sender_reciever_all', F.concat(result.user_left, F.lit("-"), result.user_right))
            .persist()
    )
    # выполняем левый анти джоин с дата фрейм "events_union_sender_receiver", что бы исключить существующие пары "sender_reciever_existing"
    result = result.join(events_union_sender_receiver, result.sender_reciever_all == events_union_sender_receiver.sender_reciever_existing, "leftanti")

    result = (
        result
            # выполняем джоин с дата фрейм "events_subscription" для "user_left" и "user_right"
            .join(events_subscription, result.user_left == events_subscription.user, "left")
            .withColumnRenamed('chans', 'chans_left')
            .drop('user')
            .join(events_subscription, result.user_right == events_subscription.user, "left")
            .withColumnRenamed('chans', 'chans_right')
            .drop('user')
            # вычисляем пересечение "user_left" и "user_right"
            .withColumn('inter_chans', F.array_intersect(F.col('chans_left'), F.col('chans_right')))
            # фильтруем строки, где размер пересечения больше 1 и user_left <> user_right
            .filter(F.size(F.col("inter_chans"))>1)
            .where("user_left <> user_right")
            # удаляем не нужные столбцы
            .drop("inter_chans", "chans_left", "chans_right", "sender_reciever_all")
            # добавляем текущую метку времени
            .withColumn("processed_dttm", F.current_timestamp())
            # вычисляем локальное время
            .withColumn('local_time', 
                    F.when( F.col('zone_id')
                           .isin('Sydney', 'Melbourne', 'Brisbane', 'Perth', 'Adelaide', 'Canberra', 'Hobart', 'Darwin'), 
                               F.from_utc_timestamp(
                                   F.col('processed_dttm'), 
                                                F.concat(F.lit('Australia/'), 
                                                         F.col('zone_id')))).otherwise(None))
            #.drop("timezone")
            .persist()
    )

    return result

# Test
#recommendations_df = recommendations(events_transform_from_df,  geo_transform_df, events_subscriptions_df, events_union_sender_receiver_df)
#recommendations_df.show()

def main() -> None:
    events_path = sys.argv[1]
    geo_path = sys.argv[2]
    output_path = sys.argv[3]
    #events_path = "/user/master/data/geo/events/"
    #geo_path = "/user/denis19/data/geo/cities/actual/geo.csv"
    #output_path = "/user/denis19/analytics/showcases/"

    conf = (SparkConf()
        .setAppName("showcase_recommendations_to_friends")
        .set("spark.executor.memory", "4g")
        .set("spark.driver.memory", "4g"))
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    geo_transform_df = geo_transform(geo_path, sql)
    events_transform_from_df = events_transform_from(events_path, sql)
    events_subscriptions_df = events_subscriptions(events_path, sql)
    events_union_sender_receiver_df = events_union_sender_receiver(events_path, sql)
    recommendations_df = recommendations(events_transform_from_df, geo_transform_df, events_subscriptions_df, events_union_sender_receiver_df)
    write = recommendations_df.write.mode('overwrite').parquet(f'{output_path}')

    return write

if __name__ == "__main__":
        main()