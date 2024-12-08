#import required packages
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql.window import Window

#create spark session
spark = SparkSession.builder.master("local")\
                        .appName("e-commerce-analyser").getOrCreate()

# Define the structure for orders dataframe 
orders_schema = StructType([ 
    StructField('Order ID', 
                StringType(), False), 
    StructField('Order Date', 
                DateType(), True), 
    StructField('CustomerName', 
                StringType(), True), 
    StructField('State', 
                StringType(), True), 
    StructField('City', 
                StringType(), True) 
]) 

#creating and exploring input dataframes:    
orders_df = spark.read.format("csv").schema(orders_schema)\
                    .option("header", True)\
                    .option("inferSchema", False)\
                    .option("dateFormat", "dd-MM-yyyy")\
                    .load("dataset/Orders.csv")
orders_df.printSchema()
orders_df.show()

order_details_df = spark.read.format("csv").option("header", True).load("dataset/OrderDetails.csv")
order_details_df.printSchema()
order_details_df.show()

sales_target_df = spark.read.format("csv").option("header", True).load("dataset/SalesTarget.csv")
sales_target_df.printSchema()
sales_target_df.show()

#Dataframe sizes:
print("Number of records:"\
        +"\nOrders: "+str(orders_df.count())\
        +"\nOrder Details: "+str(order_details_df.count())\
        +"\nSales Target: "+str(sales_target_df.count()))


#Task 1: category and subcatrgory wise order distribution, arrange by profit max to min
category_distribution_df = order_details_df.groupBy("Category","Sub-Category")\
                                .agg(functions.count("Order ID").alias("number of Orders"),\
                                     functions.sum("Quantity").alias("Total Order Quantity"),\
                                     functions.sum("Amount").alias("Total Order Amount"),\
                                     functions.sum("Profit").alias("P&L"))\
                                .orderBy("P&L", ascending=False)
category_distribution_df.show()

#Task 2: Identify most frequent customer - Assumption (CustomerName, State, City) uniquely identifies a customer
customer_frequency_df = orders_df.groupBy("CustomerName","State","City")\
                              .agg(functions.count("Order ID").alias("Number of Orders"))

customer_frequency_max_df = customer_frequency_df.withColumn("rank",\
                              functions.dense_rank().over(\
                                                  Window.orderBy(functions.col("Number of Orders").desc())))\
                              .filter(functions.col("rank") == 1)
customer_frequency_max_df.show()

#Task 3: State wise % share in orders
cnt = orders_df.count()
state_distribution_df = orders_df.groupBy("State")\
                            .agg(functions.round(functions.count("Order ID")*100/cnt,2).alias("Order Share"))\
                            .orderBy("Order Share", ascending=False)
state_distribution_df.show()

#Task 4: List all the order details of most frequent customer
frequent_customer_orders = orders_df.join(customer_frequency_max_df,\
                                          (orders_df["CustomerName"] == customer_frequency_max_df["CustomerName"])\
                                          & (orders_df["State"] == customer_frequency_max_df["State"])\
                                          & (orders_df["City"] == customer_frequency_max_df["City"]),\
                                          "leftsemi")\
                                    .join(order_details_df, orders_df["Order ID"] == order_details_df["Order ID"])\
                                    .drop(order_details_df["Order ID"])
frequent_customer_orders.show()

#Task 5: Find Categories that met monthly sales target
monthly_category_orders_df = order_details_df["Order ID","Amount","Category"]\
                                .join(orders_df["Order ID",functions.date_format("Order Date","MMM-yy").alias("Order Month")])\
                            .groupBy("Category","Order Month")\
                            .agg(functions.sum("Amount").alias("Total Amount"))                
monthly_category_orders_df.show()
result_df = monthly_category_orders_df.join(sales_target_df,\
                                            (monthly_category_orders_df["Category"] == sales_target_df["Category"])\
                                            &(monthly_category_orders_df["Order Month"] == sales_target_df["Month of Order Date"])\
                                            &(monthly_category_orders_df["Total Amount"] >= sales_target_df["Target"]))\
                                     .drop("Month of Order Date").drop(sales_target_df["Category"])
result_df.show(n=100)