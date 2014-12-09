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
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

/**
 * 
 * Helper class as a wrapper for AWS dynamoDB functionality.
 * 
 * @author Prashant Kadwe
 *
 */
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
            
            item.put(name, mapAttributeValue(value));
        }
		
		PutItemRequest itemRequest = new PutItemRequest().withTableName(tableName).withItem(item);
		
		AmazonDynamoDBClient conn = getConnection();
		conn.putItem(itemRequest);
		conn.shutdown();		
	}
	
	/**
	 * query Key Attributes
	 * 
	 * @param tableName
	 * @param hashKeyFilter
	 * @param attributeFilters
	 * @param attributesToGet
	 * @param numRecordsToGet
	 * @param sortAsc
	 * @return
	 */
		

	public List<Map<String, Object>> queryKeyAttributes(String tableName, 
			Filter hashKeyFilter,  
			List<Filter> attributeFilters, 
			List<String> attributesToGet, 
			int numRecordsToGet,
			boolean sortAsc) {
		
		Map<String, Condition> conditions = new HashMap<String, Condition>();
		
		Condition hashKeyCondition = new Condition()
			.withComparisonOperator(mapComparator(hashKeyFilter.getComparator()))
			.withAttributeValueList(mapAttributeValue(hashKeyFilter.getValue()));
		
		conditions.put(hashKeyFilter.getName(), hashKeyCondition);		

		for (Filter attributeFilter : attributeFilters) {
			
			Condition attrCondition = new Condition()
				.withComparisonOperator(mapComparator(attributeFilter.getComparator()))
				.withAttributeValueList(mapAttributeValue(attributeFilter.getValue()));		
			
			conditions.put(attributeFilter.getName(), attrCondition);	
		}
		
		QueryRequest queryRequest = new QueryRequest()
			.withTableName(tableName)
			.withKeyConditions(conditions)
			.withAttributesToGet(attributesToGet)
			.withConsistentRead(true)
			.withSelect("SPECIFIC_ATTRIBUTES")
			.withScanIndexForward(sortAsc);
		
		if (numRecordsToGet != -1) {
			queryRequest.setLimit(numRecordsToGet);
		}
		
		QueryResult queryResult  = getConnection().query(queryRequest);
		
		List<Map<String, Object>> dataList = mapItemList(queryResult.getItems());	
		
		return dataList;
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
        
        List<Map<String, Object>> dataItems = mapItemList(result.getItems());
        
        return dataItems;
    }	
    
    /**
     * Log an item
     * 
     * @param attributeList
     */
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
    
	/**
	 * map comparator to String
	 * 
	 * @param comparator
	 * @return
	 */
	private String mapComparator(int comparator) {
			
		switch (comparator) {
		
			case Filter.COMPARATOR_EQ:
				return ComparisonOperator.EQ.toString();
				
			case Filter.COMPARATOR_NE:
				return ComparisonOperator.NE.toString();			//Supported by scan API only
				
			case Filter.COMPARATOR_CONTAINS:
				return ComparisonOperator.CONTAINS.toString();
				
			case Filter.COMPARATOR_DOES_NOT_CONTAIN:			//Supported by scan API only
				return ComparisonOperator.NOT_CONTAINS.toString();
				
			case Filter.COMPARATOR_BEGINS_WITH:					//Supported by scan API only
				return ComparisonOperator.BEGINS_WITH.toString();
				
			case Filter.COMPARATOR_GT:
				return ComparisonOperator.GT.toString();
				
			case Filter.COMPARATOR_LT:
				return ComparisonOperator.LT.toString();
				
			case Filter.COMPARATOR_BETWEEN:
				return ComparisonOperator.BETWEEN.toString();
				
			default :
				throw new RuntimeException("Dynamodb doesnot support comparator - " + comparator);
		}
	}
    
	/**
	 * map object value to AttributeValue
	 * 
	 * @param value
	 * @return
	 */
	private AttributeValue mapAttributeValue(Object value) {
		
        if (value instanceof String) {
        	return (new AttributeValue().withS((String)value));
        } else if (value instanceof Integer) {
        	return(new AttributeValue().withN(String.valueOf(value)));	
        }				
        return null;
	}
	
	/**
	 * map attribute value map to object map
	 * 
	 * @param item
	 * @return
	 */
	private Map<String, Object> mapAttributeValueMap(Map<String, AttributeValue> item) {
		
		printItem(item);
		
        Map<String, Object> dataItem = new HashMap<String, Object>();
  
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
        return dataItem;
	}
	
	/**
	 * map item list to object list
	 * 
	 * @param items
	 * @return
	 */
	private List<Map<String, Object>> mapItemList(List<Map<String, AttributeValue>> items) {
		
		List<Map<String, Object>> dataItems = new ArrayList<Map<String, Object>>();
		
		for (Map<String, AttributeValue> item : items) {
			
			Map<String, Object> dataItem = mapAttributeValueMap(item);
			dataItems.add(dataItem);
		}
		return dataItems;
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
