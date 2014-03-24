package org.zero.edu.hadoop.groupsort;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Mapper 처리 결과를 Key , List<Value) 형태로 전달받아 처리하는 Reducer
 * 
 * @author SengWook
 * 
 */
public class GroupSortReducer extends Reducer<SortKey, IntWritable, SortKey, Text> {
	
	int counter = 0;

	@Override
	protected void reduce(SortKey key, Iterable<IntWritable> values,Context context) throws IOException,
			InterruptedException {
		BigInteger big = new BigInteger("0");
		int counter = 0;
		for(IntWritable value : values) {
             big = big.add(BigInteger.valueOf((long)value.get()));
             counter++;
        }
		
		context.write(key, new Text(big.toString()));
		//System.out.println("Group Finish : " + key.toString());
		counter++;
		
		context.setStatus(String.format("%d 건의 Group Key 처리", counter));
	}
}
