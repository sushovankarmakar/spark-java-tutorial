
* **25.03.2025** :
  * `Map` :
    ```java
    myRDD.map(value -> Math.sqrt(value));
    ```  
  * `Reduce` : to transform a big dataset into a single answer :
    ```java
    myRDD.reduce((val1, val2) -> va1 + val2);
    ```

  Rdd is always immutable. Once created, it can't be changed.
