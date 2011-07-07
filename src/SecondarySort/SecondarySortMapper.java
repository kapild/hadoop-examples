package SecondarySort;
import java.io.IOException;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import CustomKey.IntPair;



public class SecondarySortMapper extends  Mapper<LongWritable, Text, IntPair, IntWritable> {
	
	private String [] tokens = null;
	private IntWritable ONE = new IntWritable(1);


	@Override
	public void map(LongWritable key, Text value,
			Context context)
			throws IOException , InterruptedException{

		if(value!=null){
			tokens = value.toString().split("\\s+") ;
			System.out.println("tokens[0]"+ tokens[0] + "tokens[1]"+ tokens[1]);
				context.write(new IntPair(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1])), ONE);
					
		}
	}



}
