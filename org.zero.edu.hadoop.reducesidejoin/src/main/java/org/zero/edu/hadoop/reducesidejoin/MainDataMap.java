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
public class MainDataMap extends Mapper<LongWritable, Text, Text, Text> {

	// 처리 Line 수
	private int lineCounter;

	// 입력 파일 명
	private String inputFileName;

	public static final String JOIN_TAG = "M";

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
		// line Data를 Tab문자를 기준으로 분리
		SortKey skey = new SortKey(); 				// Reducer에게 전달할 Key 
		// 0 GROUP , 1 TYPE , 2 PRICE
		String[] split = value.toString().split("\t");
		if(split != null && split.length > 0) {
			String groupId = split[0];
			
			tkey.set(groupId +"_" + JOIN_TAG);
			tvalue.set(split[2]);
			context.write(tkey, tvalue);
		}
		
	}

	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		inputFileName = context.getConfiguration().get("mapred.input.dir");
		System.out.println(inputFileName + " File Process");
	}
}
