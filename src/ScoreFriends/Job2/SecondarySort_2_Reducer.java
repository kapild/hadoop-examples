package ScoreFriends.Job2;

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

import CustomKey.IntPair;
import CustomKey.TextPair;



public class SecondarySort_2_Reducer extends  Reducer<TextPair, TextPair, Text, TextPair> {

	
	@Override
	protected void reduce(TextPair key, Iterable<TextPair> values, Context context)
			throws IOException, InterruptedException {

		Iterator<TextPair> it = values.iterator();

		
		for(;it.hasNext();){
			context.write(key.getFirst(), it.next());
		}
		
	}

	
    
}

