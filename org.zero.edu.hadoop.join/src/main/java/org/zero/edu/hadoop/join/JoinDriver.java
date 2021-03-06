package org.zero.edu.hadoop.join;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class JoinDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		if (args.length < 2) {
			System.out.println("Usage : JoinDriver <input> <output> <cacheFile>");
			System.exit(2);
		}

		conf.set("mapred.child.java.opts", "-Xmx1024m");
		Job job = new Job(conf, "Join");

		job.setJarByClass(JoinDriver.class);
		// 결과 키 클래
		job.setOutputKeyClass(SortKey.class);
		
		DistributedCache.addCacheFile(new Path(args[2]).toUri(), job.getConfiguration());
		
		// 결과 값 클래
		job.setOutputValueClass(Text.class);

		// 매퍼클래스
		job.setMapperClass(GroupSortMap.class);

		// 리듀서 클래스
		job.setReducerClass(GroupSortReducer.class);

		// 입력 형태
		job.setInputFormatClass(TextInputFormat.class);

		// 출력형태
		job.setOutputFormatClass(TextOutputFormat.class);

		// conf.setp
		job.setMapOutputKeyClass(SortKey.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 입력 파일 경로 지정
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// 출력 파일 경로 지정
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
