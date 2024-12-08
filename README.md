# e-commerce-analysis
# Overview:
This project is about exploring and analysing E-commerce data. This primarily includes leveraging Apache Spark Dataframe API, joins, functions and aggregations to generate summarized results.

# Tasks and Results:
# Input Data:
1. Orders:
   columns - Order ID, Order Date, CustomerName, State,City
   count - 500
3. Order Details:
   columns - Order ID, Amount, Profit, Quantity, Category, Sub-Category
   count - 1500
4. Sales Target:
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
![image](https://github.com/user-attachments/assets/5d536650-eac5-404e-a4ea-11a239b48bfb)


#Task 4: List all the order details of most frequent customer
Result:
![image](https://github.com/user-attachments/assets/2eb40d1d-00f4-4710-b5d7-65ce3925be78)


#Task 5: Find Categories that met monthly sales target
Result:
![image](https://github.com/user-attachments/assets/69224779-111e-44b9-b0a9-fb02da0f9639)

