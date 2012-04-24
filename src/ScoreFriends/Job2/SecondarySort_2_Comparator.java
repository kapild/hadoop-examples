package ScoreFriends.Job2;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import CustomKey.TextIntPair;


public class SecondarySort_2_Comparator extends WritableComparator{

	protected SecondarySort_2_Comparator() {
		super(TextIntPair.class,true);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TextIntPair pair1 = (TextIntPair)a;
		TextIntPair pair2 = (TextIntPair)b;
		
		int code =  pair1.getFirst().compareTo(pair2.getFirst());
		if(code!=0)
			return code;
		return -pair1.getSecond().compareTo(pair2.getSecond());

	}
	
	
	

}
