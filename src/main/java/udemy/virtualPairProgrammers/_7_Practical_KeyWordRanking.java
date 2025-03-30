package udemy.virtualPairProgrammers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class _7_Practical_KeyWordRanking {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setAppName("keyword-ranking")
                .setMaster("local[*]");

        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {

            // Steps
            // 1. Load the file into RDD -> textFile()
            // 2. Filtering all noisy lines i.e. timestamps, empty lines and turning all of them in lowercase -> map()
            // 3. Converting lines into words -> flatmap()
            // 4. Filtering out the empty and boring words -> filter()
            // 5. Count up the remaining words -> reduceByKey()
            // 6. Find the top 10 most frequently used words -> mapToPair(), sortByKey(), take()

            String inputFileDockerCourseSubtitles = "src/main/resources/udemy/virtualPairProgrammers/subtitles/input-docker-course.txt";
            String inputFileSpringCourseSubtitles = "src/main/resources/udemy/virtualPairProgrammers/subtitles/input-spring-course.txt";

            JavaPairRDD<Long, String> wordFrequencies = jsc.textFile(inputFileSpringCourseSubtitles)
                    .map(line -> line.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase())
                    .flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                    .filter(word -> !word.isEmpty() && Util.isNotBoring(word))
                    .mapToPair(word -> new Tuple2<>(word, 1L)) // take PairFunction
                    .reduceByKey(Long::sum) // takes Function2
                    .mapToPair(pair -> new Tuple2<>(pair._2, pair._1))  // switching value and key so that we can sort by value
                    .sortByKey(false); // sorting in descending order

            wordFrequencies.take(10)
                    .forEach(System.out::println);
        }

        // why do sorts not work with foreach in Spark ?
    }
}
