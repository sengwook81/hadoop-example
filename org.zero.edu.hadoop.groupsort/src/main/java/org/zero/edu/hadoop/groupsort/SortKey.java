package org.zero.edu.hadoop.groupsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * 
 * 멀티키 정렬을 위한 SortKey Class
 * 사용자 , 지출 항목 , 금액 기준 으로 정렬.
 * 
 * @author SengWook
 *
 */
public class SortKey implements WritableComparable<SortKey>{

	private String groupId;
	private String type;
	
	// 바이너리 스트림 읽어서 값을 할당 
	public void readFields(DataInput in) throws IOException {
		groupId =  WritableUtils.readString(in);
	
		type = WritableUtils.readString(in);
	}

	public String toString() {
		return groupId +"\t" + type;
	}

	// 바이너리 스트림으로 값을 저장
	public void write(DataOutput out) throws IOException {

		WritableUtils.writeString(out, groupId);
	
		WritableUtils.writeString(out, type);
	}

	// 
	public int compareTo(SortKey compareKey) {
		
		int compareResult = groupId.compareTo(compareKey.getGroupId());
		if(compareResult == 0)
		{
			compareResult = type.compareTo(compareKey.getType());
		}
		return compareResult;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getGroupId() {
		return groupId;
	}

	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

}
