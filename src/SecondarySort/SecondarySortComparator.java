package SecondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import CustomKey.IntPair;

public class SecondarySortComparator extends WritableComparator{

	protected SecondarySortComparator() {
		super(IntPair.class,true);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		IntPair pair1 = (IntPair)a;
		IntPair pair2 = (IntPair)b;
		
		int code =  pair1.getFirst().compareTo(pair2.getFirst());
		if(code!=0)
			return code;
		return -pair1.getSecond().compareTo(pair2.getSecond());

	}
	
	
	

}
