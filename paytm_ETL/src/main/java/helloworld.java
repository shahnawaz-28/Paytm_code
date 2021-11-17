import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

public class helloworld{
    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]")
                .set("spark.testing.memory", "2147480000");// Delete this line when submitting to a cluster

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = sparkContext.textFile("F:\\Spark_Traning\\paytm_ETL\\nationalparks.csv");
        System.out.println("Number of lines in file = " + stringJavaRDD.count());
    }
}