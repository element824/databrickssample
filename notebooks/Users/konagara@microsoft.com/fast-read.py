# Databricks notebook source
# MAGIC %md
# MAGIC # Fast Reads from Azure SQL
# MAGIC 
# MAGIC This notebook shows how to read data from Azure SQL as fast as possibile

# COMMAND ----------

# MAGIC %md
# MAGIC Define variables used thoughout the script. Azure Key Value has been used to securely store sensitive data. More info here: [Create an Azure Key Vault-backed secret scope](https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes#--create-an-azure-key-vault-backed-secret-scope)

# COMMAND ----------

val scope = "key-vault-secrets"

val server = dbutils.secrets.get(scope, "srv001")
val database = "ApacheSpark"

val jdbcUrl = s"jdbc:sqlserver://$server.database.windows.net;database=$database;"

val connectionProperties = new java.util.Properties()
connectionProperties.put("user", dbutils.secrets.get(scope, "dbuser001"))
connectionProperties.put("password", dbutils.secrets.get(scope, "dbpwd001"))
connectionProperties.setProperty("Driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

# COMMAND ----------

# MAGIC %md
# MAGIC Read data using the most basic options

# COMMAND ----------

val li = spark.read.jdbc(jdbcUrl, "dbo.LINEITEM", connectionProperties)

# COMMAND ----------

# MAGIC %md
# MAGIC The `where` predicate can be pushed to Azure SQL automatically

# COMMAND ----------

li.select("L_PARTKEY", "L_SUPPKEY").where("L_ORDERKEY=7628996").show()

# COMMAND ----------

li.select("L_PARTKEY", "L_SUPPKEY").where("L_ORDERKEY=7628996").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC But only the `where` clause has this ability. The following query will *not* take advtange of Azure SQL ability to aggregate data

# COMMAND ----------

li.select("L_SUPPKEY", "L_EXTENDEDPRICE", "L_PARTKEY").where("L_SUPPKEY=5652").groupBy("L_PARTKEY").avg("L_EXTENDEDPRICE").explain()

# COMMAND ----------

li.select("L_SUPPKEY", "L_EXTENDEDPRICE", "L_PARTKEY").where("L_SUPPKEY=5652").groupBy("L_PARTKEY").avg("L_EXTENDEDPRICE").show()

# COMMAND ----------

# MAGIC %md
# MAGIC This is the equivalent query that we could have wrote and executed directly on Azure SQL, improving performance a lot, as only the result would have been transferred and not the whole `LINEITEM` table
# MAGIC 
# MAGIC ```
# MAGIC SELECT
# MAGIC 	AVG(L_EXTENDEDPRICE),
# MAGIC 	L_PARTKEY
# MAGIC FROM
# MAGIC 	dbo.LINEITEM
# MAGIC GROUP BY
# MAGIC 	L_PARTKEY
# MAGIC ORDER BY
# MAGIC 	L_PARTKEY
# MAGIC ```

# COMMAND ----------

li.select("L_EXTENDEDPRICE", "L_PARTKEY").groupBy("L_PARTKEY").avg("L_EXTENDEDPRICE").orderBy("L_PARTKEY").show()

# COMMAND ----------

# MAGIC %md
# MAGIC All the above limits are applicable also when using Spark SQL

# COMMAND ----------

li.createOrReplaceTempView("LINEITEM");

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC 	AVG(L_EXTENDEDPRICE),
# MAGIC 	L_PARTKEY
# MAGIC FROM
# MAGIC 	`LINEITEM`
# MAGIC GROUP BY
# MAGIC 	L_PARTKEY
# MAGIC ORDER BY
# MAGIC 	L_PARTKEY

# COMMAND ----------

# MAGIC %md
# MAGIC ## Real World Sample
# MAGIC 
# MAGIC Let's now simulate a real world scenario and see what can be done to optimize it. The following reference query *MUST* be executed on Spark as Azure SQL doesn't support yet `interval` windows. So all data must be transferred to Spark, and so we need to do it as fast as possibile

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte1 AS
# MAGIC (
# MAGIC   SELECT
# MAGIC     datediff(L_RECEIPTDATE, L_SHIPDATE) as ShipTime,
# MAGIC     L_COMMITDATE
# MAGIC   FROM
# MAGIC     `LINEITEM`
# MAGIC ), 
# MAGIC cte2 AS
# MAGIC (
# MAGIC   SELECT    
# MAGIC     AVG(ShipTime) AS AvgTimeToReceive,
# MAGIC     L_COMMITDATE
# MAGIC   FROM
# MAGIC     cte1
# MAGIC   GROUP BY
# MAGIC     L_COMMITDATE
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   AVG(AvgTimeToReceive) OVER (ORDER BY CAST(L_COMMITDATE AS TIMESTAMP) RANGE BETWEEN INTERVAL '7' Days PRECEDING AND CURRENT ROW) as 7DaysMovingAvg
# MAGIC FROM
# MAGIC   cte2

# COMMAND ----------

# MAGIC %md
# MAGIC Performance of above query is bad as the table is read using just one thread. To improve performance we can read it in parallel. As table is partitioned, we can read each partition in parallel, up to a desired maximum. 
# MAGIC 
# MAGIC Read a table in parallel is really fast, as long as you are using the partition column or the clustered index column (in this latter case Azure SQL table do not need to be partitioned to get the performance benefit)

# COMMAND ----------

Let's create the partitions:

# COMMAND ----------

val ms = 1 to 12;
val ys = 1992 to 1998;

val p = ys.map(y => ms.map(m => y * 100 + m)).flatten.map(pk => s"L_PARTITION_KEY = ${pk.toString}")

# COMMAND ----------

# MAGIC %md
# MAGIC And now read the table using the defined partitions

# COMMAND ----------

val li2 = spark.read.option("numPartitions", 16).jdbc(jdbcUrl, "dbo.LINEITEM", p.toArray, connectionProperties)
li2.createOrReplaceTempView("LINEITEM2");

# COMMAND ----------

# MAGIC %md
# MAGIC Let's test the performance of this approach

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte1 AS
# MAGIC (
# MAGIC   SELECT
# MAGIC     datediff(L_RECEIPTDATE, L_SHIPDATE) as ShipTime,
# MAGIC     L_COMMITDATE
# MAGIC   FROM
# MAGIC     `LINEITEM2`
# MAGIC ), 
# MAGIC cte2 AS
# MAGIC (
# MAGIC   SELECT    
# MAGIC     AVG(ShipTime) AS AvgTimeToReceive,
# MAGIC     L_COMMITDATE
# MAGIC   FROM
# MAGIC     cte1
# MAGIC   GROUP BY
# MAGIC     L_COMMITDATE
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   AVG(AvgTimeToReceive) OVER (ORDER BY CAST(L_COMMITDATE AS TIMESTAMP) RANGE BETWEEN INTERVAL '7' Days PRECEDING AND CURRENT ROW) as 7DaysMovingAvg
# MAGIC FROM
# MAGIC   cte2

# COMMAND ----------

# MAGIC %md
# MAGIC Another option is also to use the `$partition` system function

# COMMAND ----------

val pn = 1 to 100

val p = pn.map(i => s"$$partition.pf_LINEITEM([L_PARTITION_KEY]) = $i")

# COMMAND ----------

val li2 = spark.read.option("numPartitions", 16).jdbc(jdbcUrl, "dbo.LINEITEM", p.toArray, connectionProperties)
li2.createOrReplaceTempView("LINEITEM2b");

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte1 AS
# MAGIC (
# MAGIC   SELECT
# MAGIC     datediff(L_RECEIPTDATE, L_SHIPDATE) as ShipTime,
# MAGIC     L_COMMITDATE
# MAGIC   FROM
# MAGIC     `LINEITEM2b`
# MAGIC ), 
# MAGIC cte2 AS
# MAGIC (
# MAGIC   SELECT    
# MAGIC     AVG(ShipTime) AS AvgTimeToReceive,
# MAGIC     L_COMMITDATE
# MAGIC   FROM
# MAGIC     cte1
# MAGIC   GROUP BY
# MAGIC     L_COMMITDATE
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   AVG(AvgTimeToReceive) OVER (ORDER BY CAST(L_COMMITDATE AS TIMESTAMP) RANGE BETWEEN INTERVAL '7' Days PRECEDING AND CURRENT ROW) as 7DaysMovingAvg
# MAGIC FROM
# MAGIC   cte2

# COMMAND ----------

# MAGIC %md
# MAGIC If a range partition strategy cannot be used, you can try to take advantage of existing clustered index and ask Spark read the table in parallel, using manually generated buckets. This works well if data is evenly distributed. 
# MAGIC 
# MAGIC In case clustered index is not there, at least parallel read of table is achieved, but at the expense of a lot of resources used on Azure SQL side, as all queries will do a full table scans.

# COMMAND ----------

val li3 = spark.read
  .option("numPartitions", 16)
  .option("partitionColumn", "L_ORDERKEY")
  .option("lowerBound", 1)
  .option("upperBound", 60000000)
  .jdbc(jdbcUrl, "dbo.LINEITEM", connectionProperties)
li3.createOrReplaceTempView("LINEITEM3");

# COMMAND ----------

# MAGIC %md
# MAGIC Let's test the performance of this approach

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte1 AS
# MAGIC (
# MAGIC   SELECT
# MAGIC     datediff(L_RECEIPTDATE, L_SHIPDATE) as ShipTime,
# MAGIC     L_COMMITDATE
# MAGIC   FROM
# MAGIC     `LINEITEM3`
# MAGIC ), 
# MAGIC cte2 AS
# MAGIC (
# MAGIC   SELECT    
# MAGIC     AVG(ShipTime) AS AvgTimeToReceive,
# MAGIC     L_COMMITDATE
# MAGIC   FROM
# MAGIC     cte1
# MAGIC   GROUP BY
# MAGIC     L_COMMITDATE
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   AVG(AvgTimeToReceive) OVER (ORDER BY CAST(L_COMMITDATE AS TIMESTAMP) RANGE BETWEEN INTERVAL '7' Days PRECEDING AND CURRENT ROW) as 7DaysMovingAvg
# MAGIC FROM
# MAGIC   cte2

# COMMAND ----------

# MAGIC %md
# MAGIC If a clustered index is available, then performance are much better,and Azure SQL CPU usage is limited

# COMMAND ----------

val li4 = spark.read
  .option("numPartitions", 16)
  .option("partitionColumn", "L_ORDERKEY")
  .option("lowerBound", 1)
  .option("upperBound", 60000000)
  .jdbc(jdbcUrl, "dbo.LINEITEM_NONPARTITIONED", connectionProperties)
li4.createOrReplaceTempView("LINEITEM4");

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte1 AS
# MAGIC (
# MAGIC   SELECT
# MAGIC     datediff(L_RECEIPTDATE, L_SHIPDATE) as ShipTime,
# MAGIC     L_COMMITDATE
# MAGIC   FROM
# MAGIC     `LINEITEM4`
# MAGIC ), 
# MAGIC cte2 AS
# MAGIC (
# MAGIC   SELECT    
# MAGIC     AVG(ShipTime) AS AvgTimeToReceive,
# MAGIC     L_COMMITDATE
# MAGIC   FROM
# MAGIC     cte1
# MAGIC   GROUP BY
# MAGIC     L_COMMITDATE
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   AVG(AvgTimeToReceive) OVER (ORDER BY CAST(L_COMMITDATE AS TIMESTAMP) RANGE BETWEEN INTERVAL '7' Days PRECEDING AND CURRENT ROW) as 7DaysMovingAvg
# MAGIC FROM
# MAGIC   cte2

# COMMAND ----------

