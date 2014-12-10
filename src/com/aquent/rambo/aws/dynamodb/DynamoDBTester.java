package com.aquent.rambo.aws.dynamodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;


public class DynamoDBTester {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		DynamoDBHelper helper = new DynamoDBHelper();
		
		String tableName = "proof_of_concept";

		System.out.println("Adding items...");
		Map<String, Object> data = new HashMap<String, Object>();
		for (int i=1; i<=500; i++) {
			data.put("hash_key", "hash_key_" + new Random().nextInt(1000));
			data.put("range_key", new Random().nextInt(1000));
			data.put("gsi_uniq_key", "UNIQ_" + new Random().nextInt(1000));
			data.put("column_1", "column_1_" + new Random().nextInt(1000));
			data.put("column_2", "column_2_" + new Random().nextInt(1000));
			helper.addItem(tableName, data);
		}
		
		System.out.println("Querying on primary key...");
		List<String> attributesToGet = new ArrayList<String>();
		attributesToGet.add("hash_key");
		attributesToGet.add("range_key");
		attributesToGet.add("gsi_uniq_key");
		attributesToGet.add("column_1");
		attributesToGet.add("column_2");
		
		List<Map<String, Object>> result = helper.queryPrimaryKey(tableName, 
				new Filter("hash_key", Filter.COMPARATOR_EQ, "hash_key_99"),  
				null, 
				attributesToGet, 
				5,
				false);
		
		printItemList(result);
		
		System.out.println("Querying on index...");
		
		List<Map<String, Object>> result2 = helper.queryIndexKey(tableName, 
				"gsi_uniq_key-index",
				new Filter("gsi_uniq_key", Filter.COMPARATOR_EQ, "UNIQ_123"),  
				null, 
				attributesToGet, 
				5,
				false);
		
		printItemList(result2);
		
		System.out.println("Scanning on attributes...");
		
		List<Filter> filterList = new ArrayList<Filter>();
		filterList.add(new Filter("column_1", Filter.COMPARATOR_CONTAINS, "column"));
		List<Map<String, Object>> result3 = helper.scanAttribute(tableName, 
				filterList,  
				attributesToGet,
				5);
		
		printItemList(result3);
	}
	
	private static void printItemList(List<Map<String, Object>> itemList) {
		
		for (Map<String, Object> item  : itemList) {
			printItem(item);
		}		
	}
	
	private static void printItem(Map<String, Object> item) {
		
        for (Map.Entry<String, Object> attribute : item.entrySet()) {
            String name = attribute.getKey();
            String value = (String) attribute.getValue();			
            System.out.println(name + " - " + value);
        }
        System.out.println("---------------");
	}

}
