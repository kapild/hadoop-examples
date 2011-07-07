package CustomKey;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;


public class IntPair implements WritableComparable<IntPair> {
	
	IntWritable first;
	IntWritable second;
	
	
	
	public IntWritable getFirst() {
		return first;
	}
	public IntWritable getSecond() {
		return second;
	}
	
	public IntPair(){
		
		first = new IntWritable();
		second  = new IntWritable();
	}
	public IntPair(int first, int second){
		
		this(new IntWritable(first), new IntWritable(second));
	}
	
	public IntPair(IntWritable first, IntWritable second){
		this.first = first;
		this.second = second;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
		
	}
	
	@Override
	public int compareTo(IntPair that) {
		int cmp = first.compareTo(that.first);
		if(cmp==0){
			cmp = second.compareTo(that.second);
		}
		return cmp;
	}
	@Override
	public boolean equals(Object obj) {
		
		if (obj instanceof IntPair){
			IntPair that = (IntPair)obj;
			return (first.equals(that.first) && second.equals(that.second));
		}
		
		return false;
	}
	@Override
	public int hashCode() {
		return first.hashCode()  + 167 * second.hashCode();
	}
	@Override
	public String toString() {
		return first.toString() + "-" + second.toString();
	}
}
