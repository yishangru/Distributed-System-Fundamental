Scala Tutorial (Spark)

**Ref: https://www.analyticsvidhya.com/blog/2017/01/scala/**

Scala is an object-oriented programming language. It gets source code and generates Java bytecode that can be executed independently on any standard JVM.

- Installing Scala
	First install Java and then install Scala

- Basic Terms in Scala
	1. **Object**
	2. **Class**
	3. **Method**
	4. **Closure** - any function that closes over the environment in which it's defined, with return value depends on the variables declared outside the closure


- Basic Syntax (variable)
	1. Mutable variable - var Var1 : String = "test" / var Var1 = "test"
	2. Immutable variable - val Var2 : String = "test"


- Basic Syntax (if - else)
	Same as Java, C++

- Basic Syntax (iteration)
```
for ( a <- 1 to 10) {
	println("test");
}

for (i <- 1 to 5; j <- 1 to 4) println(s"($i, $j)")
# access variable use $ and s for string

for (i <- 1 to 5; j <= 1 to 4 if i == j) println(s"($i, $j)")
# add condition when creating loop

val activeCustomer = for (customer <- custList if customer.isActive()) yield(customer)
# use yield keyword to generate vector (list)
```


- Basic Syntax (function declare)
```
def functionName ([list of parameters]) : [return type]
```


- Basic Syntax (function definition)
```
def functionName ([list of parameters]) : [return type] = {
   function body
   return [expr]
}

def addInt(a:Int, b:Int = 10) : Int = {
	var sum:Int = 0
    sum = a + b
   	return sum
}
```


- Basic Data Structure
	1. Array: var name = Array("test1", "test2", "test3", "test4") | var name:Array[String] = new Array[String](3) | var name = new Array[String](3) -> name(0), name
	2. List: var numbers = list(1, 2, 3, 4, 5)
	3. Set: var numbers = Set(1, 2, 3, 4, 5)
	4. Tuple: var hostPort = ("localhost", 80) -> hostPort._1 or hostPort._2
	5. Map: Map(1 -> 2)


- Basic Function
	1. zip: List(1, 2, 3).zip(List("a", "b", "c")) -> List((1, a), (2, b), (3, c))
	2. partition: List(1, 2, 3, 4).partition(_ % 2 == 0) -> (List(1, 3), List(2, 4))
	3. reduce: List(1, 2, 3, 4).reduce(_ + _) -> 10

- Scala with Spark
	1. RDD is a collection of elements, that can be divided across multiple nodes in a cluster for parallel processing:
```
val data = Array(1, 2, 3, 4, 5)
val distData = sc.parallelize(data)
distData.collect() -> Array[Int] = Array(1, 2, 3, 4, 5)

val lines = sc.textFile("text.txt")
lines.take(2) -> first two lines
lines.count() -> count lines number
val Length = lines.map(s => s.length)
Length.collect() -> Array[Int] = Array(1, 2, 3, 4, 5)
val totalLength = Length.reduce((a, b) => a + b)

val counts = lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((x, y) => x + y) 
# (word1, [1, 1, 1]), (word2, [1, 1, 1, 1]), x and y for word count #
// flatmap can produce multiple output values with 1 to n; map with 1 to 1
```


- DataFrame in Spark with Scala
```
val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("test.csv")
# val df = spark.read.json(".....")

df.take(10).foreach(println(_)) # take 10 rows and print

df.cache() # cache data for fast reuse

df = df.dropna() # drop rows with missing values

df.columns -> Array[String] = Array(User_ID, ...)

df.count() -> number of rows

df.printSchema() -> print type for each column

df.show(2) -> display first 2 rows

df.select("Age").show(10)

df.filter(df("Purchase") >= 10000).select("Purchase").show(10)

df.groupBy("Age").count().show()
```

- Write Expression in variable
```
val x = {val a = 100; val b = 300; a + b}

val x = {
	val a = 100
	val b = 200
	a + b
}
```

- Lazy Declare in Scala
Lazy values are useful for delaying costly intialization instructions and does not give error on initialization
```
lazy val x = (1 to 10000).toList  # not actually generate
x.reduce(_ + _)  # variable is generated and calculated
```

- Collections in Scala
	1. Array (fix size)
```
val arr = new Array[Int](10)  # 0 as initialization
arr(1) = 100
arr.foreach(println)
```
	2. ArrayBuffer (variable length)
```
import scala.collection.mutable.ArrayBuffer 
var arr = new ArrayBuffer[Int]()
arr += 100
arr += (300, 400, 500)  # add elements
arr ++= Array(600, 700, 800)  # append collection
```
	3. Map
```
import scala.collection.mutable.Map
import scala.collection.immutable.Map
map.get(1)
map.getOrElse(1, "Key Missing")
map += (1 -> "Jenny")
```
	4. Tuple
```
val t = (100, "John")
t._1  # tuple index start from 1
```