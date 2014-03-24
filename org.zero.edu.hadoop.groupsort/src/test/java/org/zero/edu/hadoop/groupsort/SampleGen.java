package org.zero.edu.hadoop.groupsort;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class SampleGen {
	public static void main(String[] args) throws IOException {
		String[] types = {"A","B","C","D","E"};
		String[] groups = {"Group1","Group2","Group3","Group4","Group5"};
		File dataFile = new File("E:/IDE/kepler-jee_x64/workspace/org.zero.edu.hadoop.groupsort/src/main/resources/data");
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(dataFile));
		
		Random rand = new Random();
		for(int i = 0;i<1000;i++) {
			String groupVal = groups[rand.nextInt(groups.length)];
			String typeVal = types[rand.nextInt(types.length)];
			int priceVal = (rand.nextInt(19) + 1) * 10000;
			System.out.println(groupVal + "\t" + typeVal + "\t" + priceVal);
			bw.write(groupVal + "\t" + typeVal + "\t" + priceVal +"\n");
		}
		
		bw.close();
	}
}
