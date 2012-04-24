package ScoreFriends.Job1;


import java.io.IOException;

import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.bloom.Key;

import CustomKey.TextPair;



public class ReduceSide_1_Reducer extends  Reducer<TextPair, Text, Text, TextPair> {

	private Text SCORE = new Text();
    
	@Override
	protected void reduce(TextPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Iterator<Text> it = values.iterator();

		SCORE.set(it.next());
		
		TextPair friend = key;
		Text user = null;
		for(;it.hasNext();){
			user = it.next();
			context.write(user, new TextPair(friend.getFirst(), SCORE ));
		}
		
	}

	
    
}

