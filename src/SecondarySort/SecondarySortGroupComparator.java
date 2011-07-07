package SecondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import CustomKey.IntPair;

public class SecondarySortGroupComparator extends WritableComparator{

	protected SecondarySortGroupComparator() {
		super(IntPair.class,true);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		IntPair pair1 = (IntPair)a;
		IntPair pair2 = (IntPair)b;

	
		return pair1.getFirst().compareTo(pair2.getFirst());
		
	}
	
	
	

}
