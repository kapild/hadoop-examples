package ReduceSideJoin;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Partitioner;

import CustomKey.TextPair;

public class ReduceSideJoinPartitioner extends Partitioner<TextPair, Text> 
{

 
	@Override
	public int getPartition(TextPair key, Text value, int numOfPartitions) {
		return (key.getFirst().hashCode())%numOfPartitions;
	}
	
}
