package com.aquent.rambo.aws.dynamodb;

public class Filter {
	
	public final static int COMPARATOR_EQ = 1;
	public final static int COMPARATOR_LT = 2;
	public final static int COMPARATOR_GT = 3;
	public final static int COMPARATOR_BETWEEN = 4;
	public final static int COMPARATOR_NE = 5;
	public final static int COMPARATOR_CONTAINS = 6;
	public final static int COMPARATOR_DOES_NOT_CONTAIN = 7;
	public final static int COMPARATOR_BEGINS_WITH = 8;
	
	String name;
	int comparator;
	Object value;	
	
	public Filter(String name, int comparator, Object value) {
		this.name = name;
		this.comparator = comparator;
		this.value = value;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getComparator() {
		return comparator;
	}
	public void setComparator(int comparator) {
		this.comparator = comparator;
	}
	public Object getValue() {
		return value;
	}
	public void setValue(Object value) {
		this.value = value;
	}

}
