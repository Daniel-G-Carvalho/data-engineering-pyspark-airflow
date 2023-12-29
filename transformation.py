#import required libraries
import pyspark.sql
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Access environment variables
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
jdbc_driver_location = os.getenv("JDBC_DRIVER_LOCATION")
driver_type = os.getenv("DRIVER_TYPE")


#create spark session
spark = pyspark.sql.SparkSession \
   .builder \
   .appName("Python Spark SQL basic example") \
   .config('spark.driver.extraClassPath', jdbc_driver_location) \
   .getOrCreate()

#read movies table from db using spark
def extract_movies_to_df():
    #read table from db using spark jdbc
    movies_df = spark.read \
    .format("jdbc") \
    .option("url", db_host) \
    .option("dbtable", "movies") \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", driver_type) \
    .load()
    return movies_df

#read users table from db using spark
def extract_users_to_df():
    users_df = spark.read \
        .format("jdbc") \
        .option("url", db_host) \
        .option("dbtable", "users") \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", driver_type) \
        .load()
    return users_df


def transform_avg_ratings(movies_df, users_df):
    # transforming tables
    avg_rating = users_df.groupBy("movie_id").mean("rating")
    df = movies_df.join(
    avg_rating,
    movies_df.id == avg_rating.movie_id
    )
    df = df.drop("movie_id")
    return df


#load transformed dataframe to the database
def load_df_to_db(df):
    mode = "overwrite"
    url = db_host
    properties = {"user": db_user,
                  "password": db_password,
                  "driver": driver_type
                  }
    df.write.jdbc(url=url,
                  table = "avg_ratings",
                  mode = mode,
                  properties = properties)

if __name__ == "__main__":
    movies_df = extract_movies_to_df()
    users_df = extract_users_to_df()
    #pass the dataframes to the transformation function
    ratings_df = transform_avg_ratings(movies_df, users_df)
    #load the ratings dataframe 
    load_df_to_db(ratings_df)
    #print(ratings_df.show())