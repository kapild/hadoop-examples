package SecondarySort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import CustomKey.IntPair;





public class SecondarySortDriver {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

	    // Finish indexing unique searches
	    Job job = new Job(new Configuration(), "Secondary Sort Int Pair");

	    job.setJarByClass(SecondarySortDriver.class);
	    job.setMapperClass(SecondarySortMapper.class);
	    job.setReducerClass(SecondarySortReducer.class);
	    job.setMapOutputKeyClass(IntPair.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    job.setOutputKeyClass(IntPair.class);
	    job.setOutputValueClass(IntWritable.class);

	    job.setPartitionerClass(SecondarySortPartitioner.class);
	    job.setGroupingComparatorClass(SecondarySortGroupComparator.class);
	    job.setSortComparatorClass(SecondarySortComparator.class);
	    
	    job.setOutputFormatClass(TextOutputFormat.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    job.waitForCompletion(true);

	}


}
