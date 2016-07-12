package idv.jack.spark.driver;

import idv.jack.netty.client.Client;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class SparkDriverWordCount {

	public static void main(String args[]) throws Exception{
		JavaSparkContext sc = new JavaSparkContext();
		JavaRDD<String> textFile = sc.textFile("hdfs://apache-server-a1:9000/file1.txt");
		
		JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
		  public Iterable<String> call(String s) { return Arrays.asList(s.split(" ")); }
		});
		
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
		  public Tuple2<String, Integer> call(String s) { return new Tuple2<String, Integer>(s, 1); }
		});
		
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
		  public Integer call(Integer a, Integer b) { return a + b; }
		});
		
		List<Tuple2<String, Integer>> list = counts.collect();
		
		String resultValue = "";
		for(Tuple2<String, Integer> result : list){
			//System.out.println("result:" + result._1 + "," + result._2);
			resultValue = resultValue + result._1 + "," + result._2;
		}

		//counts.saveAsTextFile("hdfs://apache-server-a1:9000/result");
		
		String host = "192.168.1.16";
		int port = 1234;
		new Client(host, port).start(resultValue);
	}
	
}
