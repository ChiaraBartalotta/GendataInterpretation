package main;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class AttributeWithValueMaxCount {
	
	private static String stringCurrent = null;
	private static long maxCurrent = 0;
	
	public static void main(String[] args) {
		String logFile = "/home/davide/Documenti/esercizi_spark/input/prova_max.txt";
		SparkConf conf = new SparkConf().setAppName("Attribute With Value Max Count");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logData = sc.textFile(logFile);
		
		JavaPairRDD<String, Long> wordOne = logData.mapToPair(new PairFunction<String, String, Long>() {

					public Tuple2<String, Long> call(String str)
							throws Exception {
						String[] comp = str.split("\t");
						return new Tuple2<String, Long>(comp[0]+"="+comp[1], new Long(1));
					}
				});
		
		JavaPairRDD<String, Long> wordTotal = wordOne.reduceByKey(new Function2<Long, Long, Long>() {

					public Long call(Long l1, Long l2) throws Exception {
						return l1 + l2;
					}
				});
		
		JavaPairRDD<String, Long> orderedByString = wordTotal.sortByKey();
		//orderedByString.
		
		JavaPairRDD<String, Long> maxEach = orderedByString.mapToPair(new PairFunction<Tuple2<String,Long>, String, Long>() {

			public Tuple2<String, Long> call(Tuple2<String, Long> tuple)
					throws Exception {
				// TODO Auto-generated method stub
				if (stringCurrent==null) {
					stringCurrent = tuple._1();
					maxCurrent = tuple._2();
					return null;
				} else if ((stringCurrent.split("=")[0]).equals(tuple._1().split("=")[0])) {
					if (tuple._2()>maxCurrent) {
						maxCurrent = tuple._2();
						stringCurrent = tuple._1();
					}
					return null;
						
				} else {
					Tuple2<String, Long> ok = new Tuple2<String, Long>(stringCurrent, maxCurrent);
					stringCurrent = tuple._1();
					maxCurrent = tuple._2();
					return ok;
				}
					
			}
			
		});
		/*JavaPairRDD<Tuple2<String, Long>, Object> orderedByString2 = orderedByString.mapToPair(new PairFunction<Tuple2<String,Long>, Tuple2<String,Long>, Object>() {

			public Tuple2<Tuple2<String, Long>, Object> call(
					Tuple2<String, Long> tuple) throws Exception {
				return new Tuple2<Tuple2<String,Long>, Object>(tuple, null);
			}
		});
		JavaPairRDD<Tuple2<String, Long>, Object> orderedFull = orderedByString2.sortByKey(new TupleComparator(), false);*/
		
	    /*JavaPairRDD<Long, String> countInKey = wordTotal.mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {


			public Tuple2<Long, String> call(Tuple2<String, Long> tuple)
					throws Exception {
				
				return new Tuple2<Long, String>(new Long(tuple._2()), tuple._1());
			}
		});
	    JavaPairRDD<Long, String> countInKeyReduce = countInKey.reduceByKey(new Function2<String, String, String>() {

			public String call(String s1, String s2) throws Exception {
				return s1+","+s2;
			}
	    	
	    });*/
	    
	    //JavaPairRDD<Long, String> ordered = countInKeyReduce.sortByKey(false);
	    
		/*Tuple2<String, Long> lineMax = wordOne.max(new Comparator<Tuple2<String,Long>>() {

			public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
				if (o1._2()>o2._2())
					return 1;
				if (o1._2()<o2._2())
					return -1;
				return 0;
			}
		});*/
		//System.out.println(maxf._1() + "\t" + maxf._2());
		
		
		List<Tuple2<String, Long>> wordTotalCouple = maxEach.collect();
		for (Tuple2<String, Long> w : wordTotalCouple) { 
			if(w!=null)
				System.out.println(w._1() + "\t" + w._2());
		}
	    /*List<Tuple2<Tuple2<String, Long>, Object>> wordTotalCouple = orderedFull.collect();
		for (Tuple2<Tuple2<String, Long>, Object> w : wordTotalCouple)
			System.out.println(w._1()._1()+"\t"+w._1()._2());*/

	}
	
	
}
