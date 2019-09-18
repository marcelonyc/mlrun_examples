import os
# Iguazio env
V3IO_USER = os.getenv('V3IO_USERNAME')
V3IO_HOME = os.getenv('V3IO_HOME')
V3IO_HOME_URL = os.getenv('V3IO_HOME_URL')

os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages mysql:mysql-connector-java:5.1.39 pyspark-shell"


# # Initiate Spark Session

# In[ ]:


from pyspark.sql import SparkSession

# METHOD I:
#   Update Spark configurations of the following two extraClassPath with the JDBC driver location
#   prior to initiating a Spark session:
#      spark.driver.extraClassPath
#      spark.executor.extraClassPath
#
# NOTE:
# If you don't connnect to mysql, replace the mysql's connector by the other database's JDBC connector 
# in the following two extraClassPath.
#
# Initiate a Spark Session
spark = SparkSession.builder.    appName("Spark JDBC to Databases - ipynb").    config("spark.driver.extraClassPath", "/v3io/users/admin/mysql-connector-java-5.1.45.jar").    config("spark.executor.extraClassPath", "/v3io/users/admin/mysql-connector-java-5.1.45.jar").getOrCreate()


# # Spark JDBC to Databases

# ## Spark JDBC to MySQL

# ### Connecting to a public mySQL instance

# In[ ]:


#Loading data from a JDBC source
dfMySQL = spark.read     .format("jdbc")     .option("url", "jdbc:mysql://mysql-rfam-public.ebi.ac.uk:4497/Rfam")     .option("dbtable", "Rfam.family")     .option("user", "rfamro")     .option("password", "")     .option("driver", "com.mysql.jdbc.Driver")     .load()

#dfMySQL.show()


# In[ ]:


dfMySQL.write.format("io.iguaz.v3io.spark.sql.kv").mode("overwrite").option("key", "rfam_id").save("v3io://users/admin/frommysql")

