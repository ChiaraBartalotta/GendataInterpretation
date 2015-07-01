package main;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

public class TupleComparator implements Comparator<Tuple2<String, Long>>, Serializable{
	
	private static final long serialVersionUID = 1L;
	
	public int compare(Tuple2<String, Long> tuple1, Tuple2<String, Long> tuple2) {
		// TODO Auto-generated method stub
		if (tuple1._1().equals(tuple2._1())) {
			return tuple1._2() < tuple2._2() ? 0 : 1;
		}
		return -1;
		
	}

}
