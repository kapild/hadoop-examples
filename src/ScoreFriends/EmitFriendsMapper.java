package ScoreFriends;
import java.io.IOException;



import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import CustomKey.TextPair;



public class EmitFriendsMapper extends  Mapper<LongWritable, Text, Text, Text> {
	
	private String [] tokens = null;

	@Override
	public void map(LongWritable key, Text value,
			Context context)
			throws IOException , InterruptedException{

		if(value!=null){
			tokens = value.toString().split("\\s+") ;
			String user = tokens[0];
			for(int i =1;i < tokens.length; i++){
				context.write(new Text(tokens[i]), new Text(user));
			}
					
		}
	}



}
