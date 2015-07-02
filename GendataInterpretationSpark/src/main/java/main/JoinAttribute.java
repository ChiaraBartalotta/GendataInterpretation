package main;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import logic.MakeJavaRDDFromMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import util.ManageMap;

public class JoinAttribute {
	
	
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Max Count");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> metaMax = sc.textFile(args[0]);
		
		/*Map<String, Long> mm = metaMax.countByValue();
		
		for(String kk : mm.keySet())
			System.out.println(kk+"\t"+mm.get(kk));*/
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
				return new Tuple2<String, String>(el[0]+"="+el[1],el[2]);
			}
			
		});
		JavaPairRDD<String, Tuple2<Long, String>> joinsOutput = metaMaxMap.join(metaAllMap).repartition(1);
        System.out.println("Joins function Output: "+joinsOutput.collect());
		//joinsOutput.saveAsTextFile(args[2]);
		
		sc.cancelAllJobs();
		sc.clearCallSite();
		sc.close();


	}
	
	
}
