package org.zero.edu.hadoop.join;

import java.awt.SplashScreen;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
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

	// JoinMap 사용 여부
	private boolean useJoinMap = false;
	
	// 입력 파일 명 
	private String inputFileName;

	// Thread Safe 
	private Hashtable<String, String> joinMap = new Hashtable<String, String>();
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
		String groupName;
		
		if(useJoinMap ) {
			System.out.println("USE JOINMAP :" +  groupId + " > " + joinMap.get(groupId));
			groupId = joinMap.get(groupId);
		}
		else 
		{
			throw new RuntimeException("Join Map Empty");
		}

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
		inputFileName = context.getConfiguration().get("mapred.input.dir");
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if(cacheFiles != null && cacheFiles.length > 0) {
			useJoinMap = true;
			String line;
			String [] tokens;
			BufferedReader br = new BufferedReader(new FileReader(cacheFiles[0].toString()));
			try{
				while((line = br.readLine()) != null) {
					tokens = line.split("\t");
					joinMap.put(tokens[0],tokens[1]);
				}
			}catch(Exception e) {
				e.printStackTrace();
			}finally {
				br.close();
				if(joinMap.size() == 0) {
					throw new RuntimeException("Null JOin Map");
				}
				System.out.println(joinMap);
			}
		}
		else {
			throw new RuntimeException("0 CacheFiles");
		}
	}

}
