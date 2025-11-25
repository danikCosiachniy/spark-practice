#!/usr/bin/env python
# coding: utf-8

# import modules

# In[1]:


import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# read the enviroments

# In[2]:


def get_config() -> dict:
    load_dotenv()
    host = os.getenv("PG_HOST")
    user = os.getenv("PG_USER")
    password = os.getenv("PG_PASSWORD")
    db_name = os.getenv("PG_DB")
    port = os.getenv("PG_HOST_PORT")
    url = f"jdbc:postgresql://{host}:{port}/{db_name}"
    res = {
        "host" : host,
        "user" : user,
        "password" : password,
        "db_name" : db_name,
        "port" : port,
        "db_url" : url
    }
    return res


# Create the session

# In[3]:


spark = SparkSession.builder.appName("spark-practice")\
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")\
            .config("spark.sql.shuffle.partitions", "8")\
            .getOrCreate() 


# create the function read_table

# In[4]:


def read_table(spark : SparkSession, table_name: str)-> DataFrame:
    config = get_config()

    reader = (spark.read.format("jdbc")\
             .option("url", config["db_url"])\
             .option("dbtable", table_name)\
             .option("user", config["user"])\
             .option("password", config["password"])\
             .option("driver", "org.postgresql.Driver"))
    return reader.load()


# Create the dataframes of needable tables 

# In[5]:


df_film = read_table(spark, "film")
df_film_category = read_table(spark, "film_category")
df_category = read_table(spark, "category")
df_actor = read_table(spark, "actor")
df_film_actor = read_table(spark, "film_actor")
df_inventory = read_table(spark, "inventory")
df_rental = read_table(spark, 'rental')
df_payment = read_table(spark, 'payment')
df_customer = read_table(spark, 'customer')
df_address = read_table(spark, 'address')
df_city = read_table(spark, "city")


# make the query: Output the number of movies in each category, sorted in descending order. 

# In[6]:


result1 = df_category.alias("c")\
    .join(df_film_category.alias("fc"), on="category_id", how="inner")\
    .groupBy("c.name")\
    .agg(F.count("fc.film_id").alias("amount"))\
    .withColumnRenamed("name", "category")\
    .select("category", "amount")\
    .orderBy(F.desc("amount"))

result1.show()


# make the query: Output the 10 actors whose movies rented the most, sorted in descending order. 

# In[7]:


result2 = df_actor.alias("a")\
                .join(df_film_actor.alias("fa"), on="actor_id", how="inner")\
                .join(df_inventory.alias("i"), on="film_id", how="inner")\
                .join(df_rental.alias("r"), on="inventory_id", how="inner")\
                .groupBy('a.actor_id', F.concat_ws(' ', F.col("a.last_name"), F.col("a.first_name")).alias('actor_name'))\
                .agg(F.count('a.actor_id').alias('amount'))\
                .orderBy(F.desc("amount"), F.col('actor_name'))\
                .limit(10)
result2.show()


# make the query: Output the category of movies on which the most money was spent. 

# In[8]:


result3 = df_category.alias("c")\
                .join(df_film_category.alias("fc"), on='category_id', how='inner')\
                .join(df_inventory.alias('i'), on='film_id', how='inner')\
                .join(df_rental.alias('r'), on='inventory_id', how="inner")\
                .join(df_payment.alias('p'), on='rental_id', how="inner")\
                .filter(F.col("p.amount") > 0)\
                .groupBy('c.category_id', 'c.name')\
                .agg(F.sum("p.amount").alias("price"))\
                .orderBy(F.desc('price'))\
                .limit(1)
result3.show()


# make the query: Output the names of movies that are not in the inventory. 

# In[9]:


result4 = df_film.alias('f')\
                .join(df_inventory.alias('i'), on='film_id', how="left_anti")\
                .select('f.film_id', 'f.title')
result4.show(100, truncate=False)


# make the query: Output the top 3 actors who have appeared most in movies in the “Children” category. If several actors have the same number of movies, output all of them. 

# In[10]:


counts = df_actor.alias('a')\
            .join(df_film_actor.alias("fa"), on="actor_id", how="inner")\
            .join(df_film_category.alias('fc'), on='film_id', how="inner")\
            .join(df_category.alias('c'), on='category_id', how="inner")\
            .where(F.col("c.name") == "Children")\
            .groupBy('a.actor_id', F.concat_ws(' ', "a.last_name", "a.first_name").alias("actor_name"))\
            .agg(F.count('fa.actor_id').alias("films_cnt"))
ranked = counts.withColumn("rnk", F.dense_rank().over(Window.orderBy(F.desc("films_cnt"))))
result5 = ranked.filter(F.col("rnk") <= 3)\
          .select("actor_id", "actor_name", "films_cnt")\
          .orderBy(F.desc("films_cnt"), "actor_name")
result5.show(30, truncate=False)


# make the query: Output cities with the number of active and inactive customers (active - customer.active = 1). Sort by the number of inactive customers in descending order. 

# In[17]:


result6 = df_city.alias("c")\
            .join(df_address.alias('a'), on='city_id', how="inner")\
            .join(df_customer.alias('cs'), on='address_id', how='inner')\
            .groupBy('c.city')\
            .agg(
                 F.sum("cs.active").alias("active_count"),
                 (F.count("*") - F.sum("cs.active")).alias("inactive_count")
                )\
            .orderBy(F.desc('inactive_count'))
result6.show(600, truncate=False)


# make the query: Output the category of movies that have the highest number of total rental hours in the cities (customer.address_id in this city), and that start with the letter “a”. Do the same for cities with a “-” symbol.

# In[15]:


def hours_by_city_category(cat_predicate=None, city_predicate=None):
    df = df_category.alias("cat")\
        .join(df_film_category.alias("fc"), on='category_id', how='inner')\
        .join(df_inventory.alias('i'), on='film_id', how='inner')\
        .join(df_rental.alias('r'), on='inventory_id', how='inner')\
        .join(df_customer.alias('cu'), on='customer_id', how='inner')\
        .join(df_address.alias("a"), on='address_id', how='inner')\
        .join(df_city.alias('c'), on='city_id')
    if cat_predicate is not None:
        df = df.filter(cat_predicate)
    if city_predicate is not None:
        df = df.filter(city_predicate)

    return df.withColumn(
            "hours",
            (F.col("r.return_date").cast("long") - F.col("r.rental_date").cast("long")) / F.lit(3600.0)
        )\
        .groupBy(
            F.col("c.city").alias("city"),
            F.col("cat.name").alias("category")
        )\
        .agg(F.sum("hours").alias("hours_total"))


a_city   = hours_by_city_category(cat_predicate=F.col("cat.name").ilike("a%"))
dash_city = hours_by_city_category(city_predicate=F.col("c.city").like("%-%"))

w = Window.partitionBy("city").orderBy(F.desc("hours_total"))

a_ranked   = a_city.withColumn("rnk", F.rank().over(w)).filter(F.col("rnk")==1)
dash_ranked = dash_city.withColumn("rnk", F.rank().over(w)).filter(F.col("rnk")==1)

result7 = a_ranked.select(
        F.lit("starts_with_a").alias("bucket"),
        "city", "category", "hours_total"
    )\
    .unionByName(
        dash_ranked.select(
            F.lit("has_dash").alias("bucket"),
            "city", "category", "hours_total"
        )
    ).orderBy("bucket", "city", "category")

result7.show(1000, truncate=False)

