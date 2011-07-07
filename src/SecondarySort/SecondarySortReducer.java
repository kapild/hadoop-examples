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

import CustomKey.IntPair;



public class SecondarySortReducer extends  Reducer<IntPair, IntWritable, IntPair, NullWritable> {

	private IntWritable SUM = new IntWritable(-1);
    
	@Override
	protected void reduce(IntPair key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		context.write(key, NullWritable.get());
		
	}

	
    
}

