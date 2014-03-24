package org.zero.edu.hadoop.reducesidejoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 파일로부터 한라인씩 읽어 들여 해당 line에 포함된 단어를 추출하는 Mapper Class
 * 
 * @author SengWook
 * 
 */
public class CodeDataMap extends Mapper<LongWritable, Text, Text, Text> {

	// 처리 Line 수
	private int lineCounter;

	// 입력 파일 명
	private String inputFileName;

	public static final String CODE_TAG = "C";

	private Text tkey = new Text();
	private Text tvalue = new Text();

	/***
	 * 
	 * key : line Number 
	 * value : line String output 
	 * context : Job 수행정보를 관리하는 Wrapper 객체
	 ***/
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] codeName = value.toString().split("\t");

		
		if (codeName != null && codeName.length > 0) {
			String joinKey = codeName[0] + "_" + CODE_TAG;
			tkey.set(joinKey);
		    tvalue.set(codeName[1]);
  			context.write(tkey, tvalue);
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		inputFileName = context.getConfiguration().get("mapred.input.dir");
		System.out.println(inputFileName + " File Process");
	}

}

