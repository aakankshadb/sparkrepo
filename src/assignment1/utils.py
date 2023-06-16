from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

#creating a function to create spark Session
def Spark_Session():
    spark = SparkSession.builder.appName("Spark_assignment_1").getOrCreate()
    return spark

#creating a function to create data frame which has contents of user.csv file
def user_data(spark):
    df_user = spark.read.csv("../../resource/user.csv",header="true",inferSchema="true")
    return df_user

#creating a function to create data frame which has contents of transaction.csv file
def transaction_data(spark):
    df_transaction=spark.read.csv("../../resource/transaction.csv",header="true",inferSchema="true")
    return df_transaction

#creating a function which will join the transaction and user data frames using inner join
def joined_data(df_user,df_trans):
    df_join = df_trans.join(df_user, df_user.user_id == df_trans.userid,"inner")
    return df_join

#creating a function that counts unique locations where each product is sold.
def unique_loc(df_join):
    product_locations_df = df_join.select("location ","product_description")
    product_unique_locations = product_locations_df.groupBy("product_description", "location ").agg(countDistinct("location ").alias("Count"))
    return product_unique_locations

#creating a function that finds products bought by each user.
def prod_per_user(df_join):
    products = df_join.groupBy('userid').agg(collect_list("product_description").alias("Bought_Products"))
    return products

#creating a function that calculates total spending done by each user on each product.
def total_spendings(df_join):
    total = df_join.groupBy('userid', 'product_description').agg(sum('price').alias("Total spendings on each user"))
    return total



