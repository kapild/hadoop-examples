
import java.io.IOException;

import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;



public class CustomIntPairReducer extends  Reducer<IntPair, IntWritable, IntPair, IntWritable> {

	private IntWritable SUM = new IntWritable();
    


	@Override
	protected void reduce(IntPair key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum= 0;
		
		Iterator<IntWritable> it = values.iterator();
		while(it.hasNext()){
			sum++;
			it.next();
		}
		SUM.set(sum);
		context.write(key, SUM);
		
	}

	
    
}

