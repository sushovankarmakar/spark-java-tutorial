[RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)

### 02.04.2025

* Reading txt file vs csv file using `javaSparkContext.textFile("filePath")` : [Explanation](src/main/java/udemy/virtualPairProgrammers/_8_BigDataExercise.md)

### 31.03.2025

### Joins

#### 1.  Inner join
```java
JavaPairRDD<Integer, Tuple2<Integer, String>> innerJoin = userVisits.join(users);
```
```
Inner join
(6, (4,  Raquel)) 
(4, (18, Doris)) 
```
----

#### 2. Left outer join
```java
JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> leftOuterJoin = userVisits.leftOuterJoin(users);
```
```
(10, (9,    Optional.empty)) 
(6,  (4,    Optional[Raquel])) 
(4,  (18,   Optional[Doris])) 
```
----
#### 3. Right outer join
```java
JavaPairRDD<Integer, Tuple2<Optional<Integer>, String>> rightOuterJoin = userVisits.rightOuterJoin(users);
```
```
(5, (Optional.empty,    Marybelle)) 
(3, (Optional.empty,    Alan)) 
(1, (Optional.empty,    John)) 
(2, (Optional.empty,    Bob)) 
(6, (Optional[4],       Raquel)) 
(4, (Optional[18],      Doris))
```
----

#### 4. Full outer join
```java
JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> fullOuterJoin = userVisits.fullOuterJoin(users);
```
```
(5,     (Optional.empty,    Optional[Marybelle])) 
(3,     (Optional.empty,    Optional[Alan])) 
(10,    (Optional[9],       Optional.empty)) 
(4,     (Optional[18],      Optional[Doris])) 
(2,     (Optional.empty,    Optional[Bob])) 
(1,     (Optional.empty,    Optional[John])) 
(6,     (Optional[4],       Optional[Raquel])) 
```

----

#### 5. Cross Join or Cartesian
```java
JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> cartesian = userVisits.cartesian(users);
```
* All the values will be paired up with every single possible combination
```
((4,  18),   (1, John)) 
((4,  18),   (3, Alan)) 
((4,  18),   (2, Bob)) 
((4,  18),   (4, Doris)) 
((4,  18),   (5, Marybelle)) 
((4,  18),   (6, Raquel)) 
((6,  4),    (2, Bob)) 
((6,  4),    (1, John)) 
((6,  4),    (4, Doris)) 
((6,  4),    (6, Raquel)) 
((6,  4),    (3, Alan)) 
((6,  4),    (5, Marybelle)) 
((10, 9),    (1, John)) 
((10, 9),    (2, Bob)) 
((10, 9),    (3, Alan)) 
((10, 9),    (4, Doris)) 
((10, 9),    (6, Raquel)) 
((10, 9),    (5, Marybelle))
```
----

### 30.03.2025

#### Practical : Keywords ranking
1. Load the file into RDD 
    ```java
    jsc.textFile(inputFile)
    ```
2. Filtering all noisy lines i.e. timestamps, empty lines and turning all of them in lowercase
    ```java
    .map(line -> line.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase())
    ```
3. Converting lines into words
   ```java
    .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
    ```
4. Filtering out the empty AND boring words
   ```java
    .filter(word -> !word.isEmpty() && Util.isNotBoring(word))
    ```
5. Count up the remaining words
   ```java
    .mapToPair(word -> new Tuple2<>(word, 1L))
    .reduceByKey((v1, v2) -> v1 + v2)
    ```
6. Find the top 10 most frequently used words
    ```java
    .mapToPair(pair -> new Tuple2<>(pair._2, pair._1)) // switching value and key so that we can sort by value
    .sortByKey(false)
    .take(10)
    ```
7. Final output
    ```java
    JavaPairRDD<Long, String> wordFrequencies = jsc.textFile(inputFileSpringCourseSubtitles)
        .map(line -> line.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase())
        .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
        .filter(word -> !word.isEmpty() && Util.isNotBoring(word))
        .mapToPair(word -> new Tuple2<>(word, 1L)) // take PairFunction
        .reduceByKey((v1, v2) -> v1 + v2) // takes Function2
        .mapToPair(pair -> new Tuple2<>(pair._2, pair._1))  // switching value and key so that we can sort by value
        .sortByKey(false); // sorting in descending order

    wordFrequencies
        .take(10)
        .forEach(System.out::println);
    ```

### 29.03.2025

#### Reading input files from disk
* In big data, we usually read from a distributed file system like s3 or hadoop
* when we call textFile, it doesn't store the entire file into the memory
* Instead, textFile() asked driver to inform its workers to load parts (partitions) of this input file
```java
JavaRDD<String> inputRDD = jsc.textFile("...input-docker-course.txt");
```

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
