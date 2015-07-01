package logic;

import java.io.Serializable;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class MakeJavaRDDFromMap implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public JavaPairRDD<String, String> mapStringToString(JavaRDD<String> logData) {
		JavaPairRDD<String, String> stringToString = logData.mapToPair(new PairFunction<String, String, String>() {

			public Tuple2<String, String> call(String str)
					throws Exception {
				String[] comp = str.split("\t");
				return new Tuple2<String, String>(comp[0], comp[1]);
			}
		});
		return stringToString;
	}
	
	public JavaPairRDD<String, Long> mapStringToLong(JavaRDD<String> logData) {
		JavaPairRDD<String, Long> stringToLong = logData.mapToPair(new PairFunction<String, String, Long>() {

			public Tuple2<String, Long> call(String str)
					throws Exception {
				String[] comp = str.split("\t");
				return new Tuple2<String, Long>(comp[0]+"="+comp[1], new Long(1));
			}
		});
		return stringToLong;
	}
	
	
}
