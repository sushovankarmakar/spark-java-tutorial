package udemy.virtualPairProgrammers;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class _10_SparkSQL {

    public static void main(String[] args) {

        String inputFilePath = "src/main/resources/udemy/virtualPairProgrammers/exams/students.csv";

        SparkConf sparkConf = new SparkConf()
                .setAppName("spark-sql")
                .setMaster("local[*]");

        SparkSession sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        // Dataset is an abstraction which represents our data
        // we're still working with RDD objects, these objects are hidden into the Dataset object.
        Dataset<Row> inputData = sparkSession.read()
                .option("header", "true")
                .csv(inputFilePath);

        inputData.show();

        System.out.println("Total count : " + inputData.count()); // this is a distributed count, spread across multiple nodes

        // config("spark.sql.warehouse.dir", "file:///c:/tmp/") -> here spark will store some files related to spark sql

        sparkSession.cloneSession();
    }
}
