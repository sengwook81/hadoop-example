package org.zero.edu.hadoop.reducesidejoin;

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
public class JoinReduce extends Reducer<Text, Text, Text, Text> {
	
	int counter = 0;

	private Text outputKey = new Text();
	private Text outputValue = new Text();
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException,
			InterruptedException {
		String TAG = key.toString().split("_")[1];
		
		for(Text val : values) {
			if(TAG.equals(CodeDataMap.CODE_TAG))
			{
				outputKey.set(val);
			}
			else if(TAG.equals(MainDataMap.JOIN_TAG)) {
				outputValue.set(val);
				context.write(outputKey, outputValue);
			}
		}
	}
}
