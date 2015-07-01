package main;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class AttributeWithValueMaxCount2 {
	
	
	
	public static void main(String[] args) {
		//String logFile = "/home/davide/Documenti/esercizi_spark/input/prova_max.txt";
		SparkConf conf = new SparkConf().setAppName("Max Count");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> logData = sc.textFile(args[0]);
		
		JavaPairRDD<String, String> wordOne = logData.mapToPair(new PairFunction<String, String, String>() {

					public Tuple2<String, String> call(String str)
							throws Exception {
						String[] comp = str.split("\t");
						return new Tuple2<String, String>(comp[0], comp[1]);
					}
				});
		
		JavaPairRDD<String, String> wordTotal = wordOne.reduceByKey(new Function2<String, String, String>() {

					public String call(String l1, String l2) throws Exception {
						return l1 +"#"+ l2;
					}
				});
		
		JavaPairRDD<String, Long> mapToMax = wordTotal.mapToPair(new PairFunction<Tuple2<String,String>, String, Long>() {

			public Tuple2<String, Long> call(Tuple2<String, String> tuple)
					throws Exception {
				HashMap<String, Long> mapCount = new HashMap<String, Long>();
				String[] sec = tuple._2().split("#");
				for(String el : sec) {
					if (mapCount.containsKey(el)) {
						long num = mapCount.get(el);
						num +=1;
						mapCount.put(el, num);
					} else
						mapCount.put(el, new Long(1));
				}
				long max = 0;
				String maxCurrent = null;
				//almeno uno ci deve stare
				for(String key : mapCount.keySet()) {
					if (mapCount.get(key)>max) {
						max = mapCount.get(key);
						maxCurrent = key;
					}
				}
				if (maxCurrent!=null)
					return new Tuple2<String, Long>(tuple._1()+"="+maxCurrent, mapCount.get(maxCurrent));
				else
					return new Tuple2<String, Long>(tuple._1()+"=null", new Long(0));

			}
		});
		
		
		List<Tuple2<String, String>> wordTotalCouple = wordTotal.collect();
		for (Tuple2<String, String> w : wordTotalCouple) { 
			
				System.out.println(w._1() + "\t" + w._2());
		}
		List<Tuple2<String, Long>> wordTotalCouple2 = mapToMax.collect();
		for (Tuple2<String, Long> w : wordTotalCouple2) { 
			
				System.out.println(w._1() + "\t" + w._2());
		}
	    /*List<Tuple2<Tuple2<String, Long>, Object>> wordTotalCouple = orderedFull.collect();
		for (Tuple2<Tuple2<String, Long>, Object> w : wordTotalCouple)
			System.out.println(w._1()._1()+"\t"+w._1()._2());*/
		sc.cancelAllJobs();
		sc.clearCallSite();
		sc.close();

	}
	
	
}
