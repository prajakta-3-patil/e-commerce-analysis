# e-commerce-analysis
# Overview:
This project is about exploring and analysing E-commerce data. This primarily includes leveraging Apache Spark Dataframe API, joins, functions and aggregations to generate summarized results.

# Tasks and Results:
# Input Data:
1. Orders:
   columns - Order ID, Order Date, CustomerName, State,City
   count - 500
2. Order Details:
   columns - Order ID, Amount, Profit, Quantity, Category, Sub-Category
   count - 1500
3. Sales Target:
   columns - Month of Order Date, Category, Target
   count - 36

# Task 1: category and subcatrgory wise order distribution, arrange by profit max to min
Result:
+-----------+----------------+----------------+--------------------+------------------+-------+
|   Category|    Sub-Category|number of Orders|Total Order Quantity|Total Order Amount|    P&L|
+-----------+----------------+----------------+--------------------+------------------+-------+
|Electronics|        Printers|              74|               291.0|           58252.0| 5964.0|
|  Furniture|       Bookcases|              79|               297.0|           56861.0| 4888.0|
|Electronics|     Accessories|              72|               262.0|           21728.0| 3559.0|
|   Clothing|        Trousers|              39|               135.0|           30039.0| 2847.0|
|   Clothing|           Stole|             192|               671.0|           18546.0| 2559.0|
|Electronics|          Phones|              83|               304.0|           46119.0| 2207.0|
|   Clothing|     Hankerchief|             198|               754.0|           14608.0| 2098.0|
|   Clothing|         T-shirt|              77|               305.0|            7382.0| 1500.0|
|   Clothing|           Shirt|              69|               271.0|            7555.0| 1131.0|
|  Furniture|     Furnishings|              73|               310.0|           13484.0|  844.0|
|  Furniture|          Chairs|              74|               277.0|           34222.0|  577.0|
|   Clothing|           Saree|             210|               782.0|           53511.0|  352.0|
|   Clothing|        Leggings|              53|               186.0|            2106.0|  260.0|
|   Clothing|           Skirt|              64|               248.0|            1946.0|  235.0|
|   Clothing|           Kurti|              47|               164.0|            3361.0|  181.0|
|Electronics|Electronic Games|              79|               297.0|           39168.0|-1236.0|
|  Furniture|          Tables|              17|                61.0|           22614.0|-4011.0|
+-----------+----------------+----------------+--------------------+------------------+-------+

#Task 2: Identify most frequent customer - Assumption (CustomerName, State, City) uniquely identifies a customer
Result:
+------------+--------------+------+----------------+----+
|CustomerName|         State|  City|Number of Orders|rank|
+------------+--------------+------+----------------+----+
|     Sheetal|Madhya Pradesh|Indore|               4|   1|
+------------+--------------+------+----------------+----+

#Task 3: State wise % share in orders
Result:
+-----------------+-----------+
|            State|Order Share|
+-----------------+-----------+
|   Madhya Pradesh|       20.2|
|      Maharashtra|       18.0|
|        Rajasthan|        6.4|
|          Gujarat|        5.4|
|           Punjab|        5.0|
|            Delhi|        4.4|
|      West Bengal|        4.4|
|    Uttar Pradesh|        4.4|
|        Karnataka|        4.2|
|          Kerala |        3.2|
|            Bihar|        3.2|
|         Nagaland|        3.0|
|   Andhra Pradesh|        3.0|
| Himachal Pradesh|        2.8|
|Jammu and Kashmir|        2.8|
|          Haryana|        2.8|
|              Goa|        2.8|
|           Sikkim|        2.4|
|       Tamil Nadu|        1.6|
+-----------------+-----------+

#Task 4: List all the order details of most frequent customer
Result:
+--------+----------+------------+--------------+------+-------+------+--------+-----------+------------+
|Order ID|Order Date|CustomerName|         State|  City| Amount|Profit|Quantity|   Category|Sub-Category|
+--------+----------+------------+--------------+------+-------+------+--------+-----------+------------+
| B-25685|2018-06-10|     Sheetal|Madhya Pradesh|Indore|  51.00|  7.00|       2|  Furniture| Furnishings|
| B-25685|2018-06-10|     Sheetal|Madhya Pradesh|Indore| 529.00|137.00|       3|Electronics|      Phones|
| B-25685|2018-06-10|     Sheetal|Madhya Pradesh|Indore| 264.00|-30.00|       3|  Furniture| Furnishings|
| B-25685|2018-06-10|     Sheetal|Madhya Pradesh|Indore|  45.00| -2.00|       4|   Clothing|       Shirt|
| B-25724|2018-07-19|     Sheetal|Madhya Pradesh|Indore| 168.00|-51.00|       2|  Furniture|   Bookcases|
| B-25827|2018-10-23|     Sheetal|Madhya Pradesh|Indore| 156.00| 21.00|       3|  Furniture|      Chairs|
| B-25842|2018-11-02|     Sheetal|Madhya Pradesh|Indore|1543.00|370.00|       8|Electronics|    Printers|
+--------+----------+------------+--------------+------+-------+------+--------+-----------+------------+

#Task 5: Find Categories that met monthly sales target
Result:
+-----------+-----------+------------+--------+
|   Category|Order Month|Total Amount|  Target|
+-----------+-----------+------------+--------+
|   Clothing|     Jul-18|   4310674.0|14000.00|
|   Clothing|     Aug-18|   4310674.0|14000.00|
|   Clothing|     Sep-18|   4171620.0|14000.00|
|   Clothing|     Jan-19|   8482294.0|16000.00|
|Electronics|     Nov-18|   7602282.0| 9000.00|
|   Clothing|     Nov-18|   6396484.0|16000.00|
|Electronics|     Dec-18|   6775947.0| 9000.00|
|  Furniture|     Dec-18|   5214421.0|11400.00|
|   Clothing|     Jun-18|   4171620.0|12000.00|
|Electronics|     Apr-18|   7271748.0| 9000.00|
|   Clothing|     Oct-18|   5979322.0|16000.00|
|   Clothing|     Feb-19|   7508916.0|16000.00|
|Electronics|     Sep-18|   4958010.0| 9000.00|
|  Furniture|     Apr-18|   5595964.0|10400.00|
|   Clothing|     Dec-18|   5701214.0|16000.00|
|Electronics|     Jul-18|   5123277.0| 9000.00|
|  Furniture|     Feb-19|   6867774.0|11600.00|
|  Furniture|     May-18|   3942611.0|10500.00|
|   Clothing|     Mar-19|   8065132.0|16000.00|
|   Clothing|     Apr-18|   6118376.0|12000.00|
|  Furniture|     Jun-18|   3815430.0|10600.00|
|Electronics|     Aug-18|   5123277.0| 9000.00|
|  Furniture|     Aug-18|   3942611.0|10900.00|
|  Furniture|     Mar-19|   7376498.0|11800.00|
|  Furniture|     Nov-18|   5850326.0|11300.00|
|  Furniture|     Oct-18|   5468783.0|11100.00|
|Electronics|     Jun-18|   4958010.0| 9000.00|
|Electronics|     May-18|   5123277.0| 9000.00|
|Electronics|     Jan-19| 1.0081287E7|16000.00|
|  Furniture|     Sep-18|   3815430.0|11000.00|
|  Furniture|     Jul-18|   3942611.0|10800.00|
|Electronics|     Mar-19|   9585486.0|16000.00|
|  Furniture|     Jan-19|   7758041.0|11500.00|
|Electronics|     Feb-19|   8924418.0|16000.00|
|   Clothing|     May-18|   4310674.0|12000.00|
|Electronics|     Oct-18|   7106481.0| 9000.00|
+-----------+-----------+------------+--------+
