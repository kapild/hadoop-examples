package ScoreFriends.Job2;

import java.io.IOException;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import CustomKey.TextIntPair;
import CustomKey.TextPair;




public class SecondarySort_2_UserFriendMapper extends  Mapper<LongWritable, Text, TextIntPair, TextPair> {
	
	private String [] tokens = null;


	@Override
	public void map(LongWritable key, Text value,
			Context context)
			throws IOException , InterruptedException{

		//input user	friend value
		if(value!=null){
			tokens = value.toString().split("\\s+") ;
			//friend vs user
			if(tokens.length!=2){
				return;
			}

			String friendToken[] = tokens[1].split("\\-");
			
			context.write(new TextIntPair(tokens[0], friendToken[1]), new TextPair(friendToken[0], friendToken[1]));
					
		}
	}



}
