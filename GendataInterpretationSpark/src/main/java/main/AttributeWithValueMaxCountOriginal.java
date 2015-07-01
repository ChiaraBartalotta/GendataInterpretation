package main;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import logic.MakeJavaRDDFromMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import util.ManageMap;

public class AttributeWithValueMaxCountOriginal {
	
	
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Max Count");
		JavaSparkContext sc = new JavaSparkContext(conf);
		//sc.clearFiles();
		JavaRDD<String> logData = sc.textFile(args[0]);
		MakeJavaRDDFromMap mjr = new MakeJavaRDDFromMap();
		JavaPairRDD<String, String> wordOne = mjr.mapStringToString(logData);
		
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
				ManageMap mm = new ManageMap();
				ArrayList<Object> contain = mm.getStringLongMax(mapCount);
				if (contain.get(0)!=null)
					return new Tuple2<String, Long>(tuple._1()+"="+((String)contain.get(0)), ((Long) contain.get(1)));
				else
					return new Tuple2<String, Long>(tuple._1()+"=null", new Long(0));

			}
		});
		
		
		/*List<Tuple2<String, String>> wordTotalCouple = wordTotal.collect();
		for (Tuple2<String, String> w : wordTotalCouple) { 
			
				System.out.println(w._1() + "\t" + w._2());
		}*/
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
