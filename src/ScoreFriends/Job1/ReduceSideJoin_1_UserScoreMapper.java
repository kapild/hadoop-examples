package ScoreFriends.Job1;
import java.io.IOException;



import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import CustomKey.TextPair;



public class ReduceSideJoin_1_UserScoreMapper extends  Mapper<LongWritable, Text, TextPair, Text> {
	
	private String [] tokens = null;

	@Override
	public void map(LongWritable key, Text value,
			Context context)
			throws IOException , InterruptedException{

		if(value!=null){
			tokens = value.toString().split("\\s+") ;
			TextPair friendKey = new TextPair(tokens[0], "0");
			Text score  = new Text(tokens[1]); 
			context.write(friendKey, score);
					
		}
	}



}
