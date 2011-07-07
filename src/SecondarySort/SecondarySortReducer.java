package SecondarySort;

import java.io.IOException;

import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.bloom.Key;

import CustomKey.IntPair;



public class SecondarySortReducer extends  Reducer<IntPair, IntWritable, IntWritable, IntWritable> {

	private IntWritable KEY_FIRST = new IntWritable();
    
	@Override
	protected void reduce(IntPair key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		KEY_FIRST.set(key.getFirst().get());
		Iterator<IntWritable> it = values.iterator();
		for(;it.hasNext();){
			context.write(KEY_FIRST, it.next());
		}
		
	}

	
    
}

