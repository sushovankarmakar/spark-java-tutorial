
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
