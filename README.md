
### 28.03.2025

#### FlatMaps :
* Map function works in `(1 input --> 1 output)` format.
* But sometimes we need function to work with `(1 input --> 0 or more outputs)` format. Here comes flatmap
```java
JavaRDD<String> words = inputRdd.flatMap(val ->
      Arrays.asList(val.split(" ")).iterator() // flatMap returns an Iterator objects
);
```

### Filter :
* filtering out
```java
// filtering out numeric words
JavaRDD<String> filteredWords = words.filter(word -> !word.matches("\\d+"));
```

--------------------------------------------------------------------
### 27.03.2025

#### Tuple : [Link](./src/main/java/udemy/_3_Tuples.java)

* Keep different kinds of data in a same RDD.
* Can contain any kinds of data, including Java objects
* It is a scala concept.
* Tuple22 is the limit

```java
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

JavaRDD<Tuple2<Integer, Double>> tupleRdd = myRDD.map(value -> new Tuple<>(value, Math.sqrt(value)));
```
--------------------------------------------------------------------
`Tuple` can store from 2 values to 22 values. <br>
To store 2 value, we need `Tuple2<Val1, Val2>`

Now storing 2 values (one key, another one is value) is so common that, <br>
We've a special Object called `PairRDD<Key, Value>`

And this `PairRDD` has some special methods to work with keys like <br>
- `aggregateByKey`
- `reduceByKey`
- `groupByKey`
--------------------------------------------------------------------

#### PairRDD : [Link](./src/main/java/udemy/_4_PairRDD.java)

* Call `mapToPair` method to get this
* It is common to store two values in a RDD, so we've `PairRDD`
* similar to Java `Map<Key, Value>` but here **key can be repeated**
* **Has extra methods like `groupByKey` and `reduceByKey` which operates specifically on key**
```java
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

JavaPairRDD<String, String> pairRDD = myRDD.mapToPair(val -> new Tuple2<>(key, value));
```
---
`groupByKey` : 
  * can lead to severe performance problems !!
  * can cause to skewness of the data

```java
import org.apache.spark.api.java.JavaPairRDD;

JavaPairRDD<String, Iterable<String>> groupByKey = pairRDD.groupByKey();
```
---

`reduceByKey` :
  * internally it does `map` and then `reduce`
    * the keys are gather together individually, before the reduction method is applied on its values.
    * values of key1 is gathered -> now apply to reduce function
    * values of key2 is gathered -> now apply to reduce function
    * and so on ...
  * performance wise, better than `groupByKey`

```java
import org.apache.spark.api.java.JavaPairRDD;

JavaPairRDD<Integer, Integer> reduceByKey = pairRDD.reduceByKey((v1, v2) -> v1 + v2);
```

#### Fluent API : [Link](./src/main/java/udemy/_4_PairRDD_WithFluentAPI.java)
* one function's output can be another function's input. so we can do chaining
```java
JavaSparkContext sc = new JavaSparkContext(sparkConf);

sc.parallelize(inputData)                                          // returns JavaRDD<String>
  .mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L)) // returns JavaPairRDD<String, Integer>
  .reduceByKey((val1, val2) -> val1 + val2)                          // returns JavaPairRDD<String, Integer>
  .foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " records")); // returns void
```
--------------------------------------------------------------------

### 25.03.2025
#### Map : [Link](./src/main/java/udemy/_2_Function_Map.java)

```java
import org.apache.spark.api.java.JavaRDD;

JavaRDD<Double> squareRoots = myRDD.map(value -> Math.sqrt(value));
```  
#### Reduce : [Link](./src/main/java/udemy/_1_Function_Reduce.java)
* to transform a big dataset into a single answer  
```java
Double sum = myRDD.reduce((val1, val2) -> va1 + val2);
```

  Rdd is always immutable. Once created, it can't be changed.
