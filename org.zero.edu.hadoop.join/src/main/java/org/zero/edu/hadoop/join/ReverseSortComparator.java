package org.zero.edu.hadoop.join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
 * 복합키 비교기.
 * @author SengWook
 *
 */
public class ReverseSortComparator extends WritableComparator {

	protected ReverseSortComparator() {
		super(SortKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		SortKey s1 = (SortKey) a;
		SortKey s2 = (SortKey) b;
		int cmp1 = s1.getGroupId().compareTo(s2.getGroupId());
		if (cmp1 != 0) {
			return cmp1 * -1;
		}

		int cmp2 = s1.getType().compareTo(s2.getType());

		return cmp2 * -1;

	}

}
