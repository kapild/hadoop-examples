package ReduceSideJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import CustomKey.TextPair;





public class ReduceSideJoinDriver {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

	    // Finish indexing unique searches
	    Job job = new Job(new Configuration(), "Reduce side join TextPair");
	    job.setJarByClass(ReduceSideJoinDriver.class);

	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ReduceSideJoinUserNameMapper.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ReduceSideJoinUserLogsMapper.class);

	    job.setReducerClass(ReduceSideReducer.class);
	    
	    job.setMapOutputKeyClass(TextPair.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

	    job.setPartitionerClass(ReduceSideJoinPartitioner.class);
	    job.setGroupingComparatorClass(ReduceSideJoinGroupComparator.class);
	    
	    job.setOutputFormatClass(TextOutputFormat.class);

	    FileOutputFormat.setOutputPath(job, new Path(args[2]));

	    job.waitForCompletion(true);

	}


}
