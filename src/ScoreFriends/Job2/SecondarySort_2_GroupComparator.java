package ScoreFriends.Job2;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import CustomKey.TextIntPair;


public class SecondarySort_2_GroupComparator extends WritableComparator{

	protected SecondarySort_2_GroupComparator() {
		super(TextIntPair.class,true);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		TextIntPair pair1 = (TextIntPair)a;
		TextIntPair pair2 = (TextIntPair)b;

	
		return pair1.getFirst().compareTo(pair2.getFirst());
		
	}
	
	
	

}
