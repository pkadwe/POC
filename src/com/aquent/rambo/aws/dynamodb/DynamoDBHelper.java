package com.aquent.rambo.aws.dynamodb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

public class DynamoDBHelper {
	
	protected final Log logger = LogFactory.getLog(getClass());
	
	/**
	 * This method is used to initialize the AmazonDynamoDBClient connection
	 */
	protected AmazonDynamoDBClient getConnection() {
		
		//get DynamoBD credentials
		AWSCredentials credentials = new BasicAWSCredentials(System.getProperty("AWSKey"), System.getProperty("AWSSecretKey"));	    	
		AmazonDynamoDBClient conn = new AmazonDynamoDBClient(credentials);
		return conn;
	}
	
	/**
	 * Add a record - Objects supported at present are String and Integer
	 * 
	 * @param tableName
	 * @param data
	 */

	public void addItem(String tableName, Map<String, Object> data) {
		
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		
        for (Map.Entry<String, Object> dataAttribute : data.entrySet()) {
            String name = dataAttribute.getKey();
            Object value = dataAttribute.getValue();
            
            if (value instanceof String) {
            	item.put(name, new AttributeValue().withS(name));
            } else if (value instanceof Integer) {
            	item.put(name, new AttributeValue().withN(name));	
            }
        }
		
		PutItemRequest itemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
		
		AmazonDynamoDBClient conn = getConnection();
		conn.putItem(itemRequest);
		conn.shutdown();		
	}
	
	public Map<String, Object> queryKey(String tableName, String hashKeyName, Object hashKeyValue, String rangeKeyName, Object rangeKeyValue, List<String> attributesToGet) {
		
		HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		if (hashKeyValue instanceof String) {
			key.put(hashKeyName, new AttributeValue().withS((String)hashKeyValue));
		} else if (hashKeyValue instanceof Integer) {
			key.put(hashKeyName, new AttributeValue().withN(String.valueOf((Integer)hashKeyValue)));			
		}
		
		if (rangeKeyName != null) {
			if (rangeKeyValue instanceof String) {
				key.put(rangeKeyName, new AttributeValue().withS((String)rangeKeyValue));
			} else if (rangeKeyValue instanceof Integer) {
				key.put(rangeKeyName, new AttributeValue().withN(String.valueOf((Integer)rangeKeyValue)));			
			}
		}
		
		GetItemRequest itemRequest = new GetItemRequest()
				.withTableName(tableName)
				.withKey(key)
				.withConsistentRead(true)
				.withAttributesToGet(attributesToGet);
		
		GetItemResult itemResult = getConnection().getItem(itemRequest);
		
		Map<String, AttributeValue> item = itemResult.getItem();
		
		Map<String, Object> data = new HashMap<String, Object>();
		
        for (Map.Entry<String, AttributeValue> attribute : item.entrySet()) {
            String name = attribute.getKey();
            Object value = attribute.getValue();
            
            data.put(name, value);
        }		
		
		return data;
	}
	
	/**
	 * Scan an attribute
	 * 
	 * @param tableName
	 * @param attributeName
	 * @param attributeValue
	 * @param attributesToGet
	 * @return
	 */
    public List<Map<String, Object>> scanAttribute(String tableName, String attributeName, String attributeValue, List<String> attributesToGet) {
    	
    	Map<String, Condition> scanFilter = new HashMap<String, Condition>();
		Condition condition = new Condition()
			.withComparisonOperator(ComparisonOperator.EQ.toString())
			.withAttributeValueList(new AttributeValue().withS(attributeValue));
		scanFilter.put(attributeName, condition);

        ScanRequest request = new ScanRequest()
        	.withTableName(tableName)
        	.withAttributesToGet(attributesToGet)
        	.withScanFilter(scanFilter);
        
        ScanResult result = getConnection().scan(request);
        
        List<Map<String, Object>> dataItems = new ArrayList<Map<String, Object>>();
        
        for (Map<String, AttributeValue> item : result.getItems()) {
            printItem(item);
            Map<String, Object> dataItem = new HashMap<String, Object>();
            dataItems.add(dataItem);
  
            for (Map.Entry<String, AttributeValue> attribute : item.entrySet()) {
                String name = attribute.getKey();
                AttributeValue value = attribute.getValue();
                
                if (value.getS() != null) {
                	dataItem.put(name, value.getS());
                } else if (value.getN() != null) {
                	dataItem.put(name, value.getN());
                } else if (value.getB() != null) {
                	dataItem.put(name, value.getB());
                }
            }          
        }
        
        return dataItems;
    }	
    
    private void printItem(Map<String, AttributeValue> attributeList) {
        for (Map.Entry<String, AttributeValue> item : attributeList.entrySet()) {
            String attributeName = item.getKey();
            AttributeValue value = item.getValue();
            logger.debug(attributeName + " "
                    + (value.getS() == null ? "" : "S=[" + value.getS() + "]")
                    + (value.getN() == null ? "" : "N=[" + value.getN() + "]")
                    + (value.getB() == null ? "" : "B=[" + value.getB() + "]")
                    + (value.getSS() == null ? "" : "SS=[" + value.getSS() + "]")
                    + (value.getNS() == null ? "" : "NS=[" + value.getNS() + "]")
                    + (value.getBS() == null ? "" : "BS=[" + value.getBS() + "] \n"));
        }
    }   

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
