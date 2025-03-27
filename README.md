

* **27.03.2025** :
  * `Tuple` : [Link](./src/main/java/udemy/_3_Tuples.java)
     * Keep different kinds of data in a same RDD.
     * Can contains any kinds of data, including Java objects
     * It is a scala concept.
     * Tuple22 is the limit
      ```java
      import scala.Tuple2;
      JavaRDD<Tuple2<Integer, Double>> tupleRdd = myRDD.map(value -> new Tuple<>(value, Math.sqrt(value)));
      ```

* **25.03.2025** :
  * `Map` : [Link](./src/main/java/udemy/_2_Function_Map.java)
    ```java
    myRDD.map(value -> Math.sqrt(value));
    ```  
  * `Reduce` : to transform a big dataset into a single answer : [Link](./src/main/java/udemy/_1_Function_Reduce.java)
    ```java
    myRDD.reduce((val1, val2) -> va1 + val2);
    ```

  Rdd is always immutable. Once created, it can't be changed.
