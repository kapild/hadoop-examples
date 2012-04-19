package ScoreFriends.Job2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import CustomKey.IntPair;
import CustomKey.TextPair;

public class SecondarySort_2_GroupComparator extends WritableComparator{

	protected SecondarySort_2_GroupComparator() {
		super(TextPair.class,true);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		TextPair pair1 = (TextPair)a;
		TextPair pair2 = (TextPair)b;

	
		return pair1.getFirst().compareTo(pair2.getFirst());
		
	}
	
	
	

}
