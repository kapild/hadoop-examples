package ScoreFriends;



import CustomKey.TextIntPair;
import CustomKey.TextPair;
import ScoreFriends.Job1.ReduceSideJoin_1_FriendsUserMapper;
import ScoreFriends.Job1.ReduceSideJoin_1_GroupComparator;
import ScoreFriends.Job1.ReduceSideJoin_1_Partitioner;
import ScoreFriends.Job1.ReduceSideJoin_1_UserScoreMapper;
import ScoreFriends.Job1.ReduceSide_1_Reducer;
import ScoreFriends.Job2.SecondarySort_2_GroupComparator;
import ScoreFriends.Job2.SecondarySort_2_Partitioner;
import ScoreFriends.Job2.SecondarySort_2_Reducer;
import ScoreFriends.Job2.SecondarySort_2_UserFriendMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;







public class FriendScoreDriver {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

	    // Finish indexing unique searches
	    Job job = new Job(new Configuration(), "Score Friends K problem");
	    job.setJarByClass(FriendScoreDriver.class);

	    String pathFriend = args[0];
	    String pathScore = args[1];
	    String pathOut = args[2];
	    MultipleInputs.addInputPath(job, new Path(pathFriend), TextInputFormat.class, EmitFriendsMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

	    String job1OutPath = pathOut + "/job1";
	    FileOutputFormat.setOutputPath(job, new Path(job1OutPath));
	    job.waitForCompletion(true);
	  //====================

	    Job job2 = new Job(new Configuration(), "Join Friends and Score");
	    job2.setJarByClass(FriendScoreDriver.class);

	    MultipleInputs.addInputPath(job2, new Path(pathScore), TextInputFormat.class, ReduceSideJoin_1_UserScoreMapper.class);
	    MultipleInputs.addInputPath(job2, new Path(job1OutPath), TextInputFormat.class, ReduceSideJoin_1_FriendsUserMapper.class);

	    job2.setMapOutputKeyClass(TextPair.class);
	    job2.setMapOutputValueClass(Text.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(TextPair.class);
	    job2.setOutputFormatClass(TextOutputFormat.class);
	    job2.setPartitionerClass(ReduceSideJoin_1_Partitioner.class);
	    job2.setGroupingComparatorClass(ReduceSideJoin_1_GroupComparator.class);
	    job2.setReducerClass(ReduceSide_1_Reducer.class);
	    String job2OutPath = pathOut + "/job2";
	    FileOutputFormat.setOutputPath(job2, new Path(job2OutPath));
	    job2.waitForCompletion(true);
	    
//====================
	    Job job3 = new Job(new Configuration(), "Sort Friends score");
	    job3.setJarByClass(FriendScoreDriver.class);

	    MultipleInputs.addInputPath(job3, new Path(job2OutPath), TextInputFormat.class, SecondarySort_2_UserFriendMapper.class);

	    job3.setMapOutputKeyClass(TextIntPair.class);
	    job3.setMapOutputValueClass(TextPair.class);
	    job3.setOutputKeyClass(Text.class);
	    job3.setOutputValueClass(TextPair.class);
	    
	    job3.setOutputFormatClass(TextOutputFormat.class);
	    
	    job3.setPartitionerClass(SecondarySort_2_Partitioner.class);
	    job3.setGroupingComparatorClass(SecondarySort_2_GroupComparator.class);
//	    job3.setSortComparatorClass(SecondarySort_2_Comparator.class);
	    job3.setReducerClass(SecondarySort_2_Reducer.class);

	    String job3OutPath = pathOut + "/job3";
	    FileOutputFormat.setOutputPath(job3, new Path(job3OutPath));
	    job3.waitForCompletion(true);
	    
	}


}
