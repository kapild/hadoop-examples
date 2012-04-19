package ScoreFriends.Job2;
import java.io.IOException;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import CustomKey.IntPair;
import CustomKey.TextPair;



public class SecondarySort_2_UserFriendMapper extends  Mapper<LongWritable, Text, TextPair, Text> {
	
	private String [] tokens = null;


	@Override
	public void map(LongWritable key, Text value,
			Context context)
			throws IOException , InterruptedException{

		//input user	friend value
		if(value!=null){
			tokens = value.toString().split("\\s+") ;
			//friend vs user
			if(tokens.length!=3){
				return;
			}
			context.write(new TextPair(tokens[0], tokens[2]), new TextPair(tokens[1], tokens[2]));
					
		}
	}



}
