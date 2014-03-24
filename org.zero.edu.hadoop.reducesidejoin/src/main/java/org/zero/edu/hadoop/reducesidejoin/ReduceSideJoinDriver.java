package org.zero.edu.hadoop.reducesidejoin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ReduceSideJoinDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		if (args.length < 2) {
			System.out.println("Usage : JoinDriver <input> <output> <cacheFile>");
			System.exit(2);
		}

		conf.set("mapred.child.java.opts", "-Xmx1024m");
		Job job = new Job(conf, "Join");

		job.setJarByClass(ReduceSideJoinDriver.class);

		// 리듀서 클래스
		job.setReducerClass(JoinReduce.class);
		// 결과 키 클래
		job.setOutputKeyClass(Text.class);
		// 결과 값 클래
		job.setOutputValueClass(Text.class);

		// 출력형태
		job.setOutputFormatClass(TextOutputFormat.class);

		// 매퍼 아웃풋 키/값 타입
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// 입력 파일 경로 지정
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MainDataMap.class);
		// 입력 파일별 매퍼 지정.
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, CodeDataMap.class);

		// 출력 파일 경로 지정
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
