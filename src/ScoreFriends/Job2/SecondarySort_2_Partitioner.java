package ScoreFriends.Job2;


import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Partitioner;

import CustomKey.TextIntPair;
import CustomKey.TextPair;


public class SecondarySort_2_Partitioner extends Partitioner<TextIntPair, TextPair> 
{

 
	@Override
	public int getPartition(TextIntPair key, TextPair value, int numOfPartitions) {
		return (key.getFirst().hashCode())%numOfPartitions;
	}
	
}
