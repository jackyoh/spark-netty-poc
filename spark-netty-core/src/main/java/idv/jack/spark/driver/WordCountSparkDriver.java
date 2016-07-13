package idv.jack.spark.driver;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCountSparkDriver extends AbstractSparkDriver{
	
	public WordCountSparkDriver(String []sparkDriverArgs){
		super(sparkDriverArgs);
	}
	
	
	public String sparkDriverLogic() throws Exception{
		
		JavaSparkContext sc = new JavaSparkContext();
		JavaRDD<String> textFile = sc.textFile(this.sparkDriverArgs[0]);
		//JavaRDD<String> textFile = sc.textFile("hdfs://apache-server-a1:9000/file1.txt");
		
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
			resultValue = resultValue + result._1 + "," + result._2 + "  ";
		}
		return resultValue;
	}
}
