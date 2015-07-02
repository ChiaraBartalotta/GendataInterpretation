package main;


import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class AddAttribute {
	
	
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Add Attribute");
		final String nameTumor = args[1];
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> logData = sc.textFile(args[0]);
		JavaPairRDD<String, String> addThird = logData.mapToPair(new PairFunction<String, String, String>() {

			public Tuple2<String, String> call(String str)
					throws Exception {
				String[] comp = str.split("\t");
				return new Tuple2<String, String>(comp[0]+"="+comp[1], nameTumor);
			}
		}).distinct();
		List<Tuple2<String, String>> wordTotalCouple = addThird.collect();
		for (Tuple2<String, String> w : wordTotalCouple)
			System.out.println(w._1() + "\t" + w._2());
		
		sc.cancelAllJobs();
		sc.clearCallSite();
		sc.close();


	}
	
	
}
