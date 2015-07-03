package main;


import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import logic.MakeJavaRDDFromMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import util.ManageMap;

public class JoinAttributeOriginal {
	
	
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Max Count");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> metaMax = sc.textFile(args[0]);
		
		
		JavaPairRDD<String, Long> metaMaxMap = metaMax.mapToPair(new PairFunction<String, String, Long>() {

			public Tuple2<String, Long> call(String ele)
					throws Exception {
				String[] el = ele.split("\t");
				return new Tuple2<String, Long>(el[0], new Long(el[1]));
			}
			
		});
		
		JavaRDD<String> metaAll = sc.textFile(args[1]);
		JavaPairRDD<String, String> metaAllMap = metaAll.mapToPair(new PairFunction<String, String, String>() {

			public Tuple2<String, String> call(String ele)
					throws Exception {
				String[] el = ele.split("\t");
				return new Tuple2<String, String>(el[0],el[1]);
			}
			
		});
		JavaPairRDD<String, Tuple2<Long, String>> joinsOutput = metaMaxMap.join(metaAllMap).repartition(1);
        //System.out.println("Joins function Output: "+joinsOutput.collect());
		//joinsOutput.saveAsTextFile(args[2]);
		Map<String, HashSet<String>> mapOutput = new HashMap<String, HashSet<String>>();
		List<Tuple2<String, Tuple2<Long, String>>> liss = joinsOutput.collect();
		for(Tuple2<String, Tuple2<Long, String>> tupl : liss) {
			HashSet<String> lst;
			if (mapOutput.containsKey(tupl._1())) 
				lst = mapOutput.get(tupl._1());
			else 
				lst = new HashSet<String>();
			lst.add(tupl._2()._2());
			mapOutput.put(tupl._1(), lst);
		}
		ManageMap mm = new ManageMap();
		mm.printMap(mapOutput);
		sc.cancelAllJobs();
		sc.clearCallSite();
		sc.close();


	}
	
	
}
