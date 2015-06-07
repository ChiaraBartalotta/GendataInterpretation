package main;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class AttributeCount {

	public static void main(String[] args) {
	
		SparkConf conf = new SparkConf().setAppName("Attribute Count");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logData = sc.textFile(args[0]);

		JavaPairRDD<String, Long> wordOne = logData.mapToPair(new PairFunction<String, String, Long>() {

					public Tuple2<String, Long> call(String str)
							throws Exception {
						String[] comp = str.split("\t");
						return new Tuple2<String, Long>(comp[0], new Long(1));
					}
				});
		

		JavaPairRDD<String, Long> wordTotal = wordOne
				.reduceByKey(new Function2<Long, Long, Long>() {

					public Long call(Long l1, Long l2) throws Exception {
						return l1 + l2;
					}
				});
		List<Tuple2<String, Long>> wordTotalCouple = wordTotal.collect();
		for (Tuple2<String, Long> w : wordTotalCouple)
			System.out.println(w._1() + "," + w._2());

	}

}
