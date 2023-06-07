from src.assignment1.utils import *

spark = Spark_Session()

df_user = user_data(spark)
print("User table")
df_user.show()

df_trans = transaction_data(spark)
print("Transaction table")
df_trans.show()

df_join = joined_data(df_user, df_trans)
df_join.show()

df_unique_location = unique_loc(df_join)
print("Unique Cities where each product is sold.")
df_unique_location.show()

prod_bought_user = prod_per_user(df_join)
print("Products bought by each user")
prod_bought_user.show()

exp_user_prod = total_spendings(df_join)
print("Total spending done by each user on each product")
exp_user_prod.show()