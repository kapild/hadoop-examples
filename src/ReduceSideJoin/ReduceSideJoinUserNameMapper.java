package ReduceSideJoin;
import java.io.IOException;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import CustomKey.IntPair;
import CustomKey.TextPair;



public class ReduceSideJoinUserNameMapper extends  Mapper<LongWritable, Text, TextPair, Text> {
	
	private String [] tokens = null;


	@Override
	public void map(LongWritable key, Text value,
			Context context)
			throws IOException , InterruptedException{

		if(value!=null){
			tokens = value.toString().split("\\s+") ;
			context.write(new TextPair(tokens[0], "1"), new Text(tokens[1]));
					
		}
	}



}
