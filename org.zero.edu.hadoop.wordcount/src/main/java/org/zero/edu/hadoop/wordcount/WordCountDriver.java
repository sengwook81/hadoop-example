package org.zero.edu.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountDriver {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		
		if(args.length != 2) 		
		{
			System.out.println("Usage : WordCountDriver <input> <output>" );
			System.exit(2);
		}
		
		conf.set("mapred.child.java.opts", "-Xmx1024m");
		Job job = new Job(conf,"WordCount");
		
		// 
		job.setJarByClass(WordCountDriver.class);
		
		// 매퍼 
		job.setMapperClass(WordCountMap.class);
		// 리듀서 
		job.setReducerClass(WordCountReducer.class);
		
		// 입력 데이터 포맷 클래스
		job.setInputFormatClass(TextInputFormat.class);
		// 출력 데이터 결과 포맷 클래스 
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// 출력 키 직렬화 클래스 
		job.setOutputKeyClass(Text.class);
		// 출력 값 직렬화 클래스 
		job.setOutputValueClass(IntWritable.class);
		
		// 입력 데이터 경로.
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		// 출력 결과 저장 경로.
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		try {
			job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
