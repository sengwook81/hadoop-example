package org.zero.edu.hadoop.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 파일로부터 한라인씩 읽어 들여 해당 line에 포함된 단어를 추출하는 Mapper Class
 * 
 * @author SengWook
 * 
 */
public class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	// 대소문자 무시 Option명
	public static final String IGNORE_CASE = "ignoreCase";

	// 대소문자 무시 여부
	private boolean bCase = false;

	// 처리 Line 수
	private int lineCounter;

	// 입력 파일 명 
	private String inputFileName;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		if (conf.get(IGNORE_CASE) != null) {
			bCase = Boolean.parseBoolean(conf.get(IGNORE_CASE));
		}
		inputFileName = conf.get("map.input.file");
	}
	
	/***
	 * 
	 * key : line Number 
	 * value : line String output 
	 * context : Job 수행정보를 담는 Wrapper 객체.
	 ***/
	@Override
	protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		// 대소문자 무시 옵션 지정시 해당 value 를 모두 소문자로 치환
		String line = (bCase) ? value.toString() : value.toString().toLowerCase();

		// line Data를 space값을 기준으로 분리
		StringTokenizer tokenizer = new StringTokenizer(line);

		Text word = new Text(); 				// Reducer에게 전달할 Key 
		IntWritable one = new IntWritable(1); 	// Reducer에게 전달할 Value 
		
		// 단어의 개수만큼 Loop 처리.
		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			context.write(word, one);
			// 총 단어수 Count
			context.getCounter("TOTAL WORDS","COUNTER").increment(1);
			
		}

		// 100 Line을 기준으로 현재 상태를 표시.
		if ((++lineCounter % 100) == 0) {
			context.setStatus(inputFileName + " 파일의 " + lineCounter + " 행 처리완");
		}
	}



}
