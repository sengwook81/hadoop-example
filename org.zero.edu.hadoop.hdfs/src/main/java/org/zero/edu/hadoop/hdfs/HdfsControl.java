package org.zero.edu.hadoop.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsControl {

	// AWS master IP
	static final String FS_DEFAULT_NAME = "hdfs://54.199.247.226:9000";

	// AWS User ID
	static final String HADOOP_USER = "ubuntu";

	public static void main(String[] args) {
		// HDFS 접속 사용자 계정명 등록.
		System.setProperty("HADOOP_USER_NAME", HADOOP_USER);
		int res = 1;

		Configuration conf = new Configuration();
		// 기본 하둡 파일시스템 주소 설정
		conf.set("fs.default.name", FS_DEFAULT_NAME);

		try {
			// 파일시스템 접속.
			FileSystem hdfs = FileSystem.get(conf);
			System.out.println("Connected HDFS FILE SYSTEM\nHome" + hdfs.getHomeDirectory());

			// 디렉토리 Path생성
			Path path = new Path("/user/sengwook");

			if (hdfs.exists(path)) {
				System.out.println("Exists Remove Recursive");
				hdfs.delete(path, true);
			} else {
				System.out.println("Create Directory");
				hdfs.mkdirs(path);
			}

			// Test용 파일 생성.
			Path hdfsFile = new Path(path, "test.txt");

			// 생성한 파일에 텍스트 Write
			FSDataOutputStream newFile = hdfs.create(hdfsFile);
			newFile.writeUTF("Hello World");
			newFile.close();

			FSDataInputStream openFile = hdfs.open(hdfsFile);

			BufferedReader reader = new BufferedReader(new InputStreamReader(openFile));
			String line = null;
			while ((line = reader.readLine()) != null) {
				System.out.println("Read From HDFS FILE : " + line);
			}
			openFile.close();

			hdfs.delete(hdfsFile, true);
			System.out.println("File Delete");

			hdfs.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}