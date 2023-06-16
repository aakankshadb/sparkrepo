from src.assignment1.utils import *
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("../../logs/assignment1.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

spark = Spark_Session()

df_user = user_data(spark)
logging.info("User table:")
df_user.show()

df_trans = transaction_data(spark)
logging.info("Transaction table")
df_trans.show()

df_join = joined_data(df_user, df_trans)
logging.info("Joined table")
df_join.show()

df_unique_location = unique_loc(df_join)
logging.info("Unique Cities where each product is sold.")
df_unique_location.show()

prod_bought_user = prod_per_user(df_join)
logging.info("Products bought by each user")
prod_bought_user.show()

exp_user_prod = total_spendings(df_join)
logging.info("Total spending done by each user on each product")
exp_user_prod.show()