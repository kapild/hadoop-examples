package SecondarySort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import CustomKey.IntPair;

public class SecondarySortPartitioner extends Partitioner<IntPair, IntWritable> 
{

 
	@Override
	public int getPartition(IntPair key, IntWritable value, int numOfPartitions) {
		// TODO Auto-generated method stub
		
		return (key.getFirst().hashCode())%numOfPartitions;
	}
	
}
