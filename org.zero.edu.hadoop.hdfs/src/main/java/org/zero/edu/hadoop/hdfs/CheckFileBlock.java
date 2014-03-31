package org.zero.edu.hadoop.hdfs;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CheckFileBlock {

	// AWS master IP
	static final String FS_DEFAULT_NAME = "hdfs://name1:9000";

	// AWS User ID
	static final String HADOOP_USER = "edu";

	public static void main(String[] args) {
		// HDFS 접속 사용자 계정명 등록.
		System.setProperty("HADOOP_USER_NAME", HADOOP_USER);
		int res = 1;

		Configuration conf = new Configuration();
		// 기본 하둡 파일시스템 주소 설O정
		conf.set("fs.default.name", FS_DEFAULT_NAME);

		try {
			// 파일시스템 접속.
			FileSystem hdfs = FileSystem.get(conf);
			FileStatus fileStatus = hdfs.getFileStatus(new Path("/user/edu/remote/test2.txt"));
			BlockLocation[] fileBlockLocations = hdfs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
			for(BlockLocation bl : fileBlockLocations) {
				System.out.println(Arrays.deepToString(bl.getHosts()));
				//System.out.println(Arrays.deepToString(bl.getNames()));
				
				System.out.println(bl.getLength() + " , " + bl.getOffset());
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
