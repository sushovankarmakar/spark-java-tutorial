package udemy;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class _2_Function_Map {

    public static void main(String[] args) {

        List<Integer> inputData = new ArrayList<>();
        inputData.add(353);
        inputData.add(12345);
        inputData.add(93);
        inputData.add(38);
        inputData.add(623);
        inputData.add(117);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf()
                .setAppName("startingSpark")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<Integer> myRDD = sc.parallelize(inputData);

        JavaRDD<Double> squareRoots = myRDD.map((value) -> Math.sqrt(value));
        squareRoots.foreach(val -> System.out.println("Sqrt val : " + val));
        squareRoots.foreach(System.out::println); // more modern syntax
    }
}
