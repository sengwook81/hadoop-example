package org.zero.edu.hadoop.groupsort;

import java.io.IOException;

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
public class GroupSortMap extends Mapper<LongWritable, Text, SortKey, IntWritable> {

	// 처리 Line 수
	private int lineCounter;

	// 입력 파일 명 
	private String inputFileName;

	/***
	 * 
	 * key : line Number 
	 * value : line String output 
	 * : Reducer에게 전달할 데이터 컬렉
	 * report : 현재 진행상태를 전달할 수 있는 리포터
	 ***/
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// line Data를 Tab문자를 기준으로 분리
		SortKey skey = new SortKey(); 				// Reducer에게 전달할 Key 
		// 0 GROUP , 1 TYPE , 2 PRICE
		String[] split = value.toString().split("\t");
		
		String groupId = split[0];
		String type = split[1];
		Integer price = Integer.parseInt(split[2]);
		
		skey.setGroupId(groupId);
		skey.setType(type);
		context.write(skey, new IntWritable(price));

		// 100 Line을 기준으로 현재 상태를 표시.
		if ((++lineCounter % 100) == 0) {
			context.setStatus(inputFileName + " 파일의 " + lineCounter + " 행 처리완");
		}
	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		inputFileName = context.getConfiguration().get("map.input.file");
	}

}
