package org.zero.edu.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Mapper 처리 결과를 Key , List<Value> 형태로 전달받아 처리하는 Reducer
 * 
 * @author SengWook
 * 
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
			InterruptedException {
		int sum = 0;
		
		// values 는 해당 단어가 노출된 횟수만큼의 Item이 포함되어 있음.
        for(IntWritable val : values) {
            sum += val.get(); // = sum += 1;
        }
        context.write(key, new IntWritable(sum));
        
        context.getCounter("WORD ITEM COUNT", " COUNT").increment(1);
	}
}
