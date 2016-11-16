package com.ibm.hursley.kappa.queries;

import java.util.Comparator;

import org.json.JSONObject;

public class SearchComparator implements Comparator<JSONObject>{

	private String sortField = null;
	private String sortOrder = null;
	
	public SearchComparator(String field, String order) {
		this.sortField = field;
		this.sortOrder = order;
	}
	
	@Override
	public int compare(JSONObject o1, JSONObject o2) {
		
		// handle empty or null values
		if((o1 == null || !o1.has(sortField) || o1.getString(sortField).length() < 1) && (o2 == null || !o2.has(sortField) || o2.getString(sortField).length() < 1)){
			return 0;
		}
		if(o1 == null || !o1.has(sortField) || o1.getString(sortField).length() < 1){
			if(sortOrder.equalsIgnoreCase("asc")){
				return -1;
			}
			else{
				return -1;
			}
		}
		if(o2 == null || !o2.has(sortField) || o2.getString(sortField).length() < 1){
			if(sortOrder.equalsIgnoreCase("asc")){
				return 1;
			}
			else{
				return 1;
			}
		}
		
		String fieldValue1 = o1.getString(sortField);
		String fieldValue2 = o2.getString(sortField);
		
		
		// standard strings
		if(sortOrder.equalsIgnoreCase("asc")){
			return fieldValue1.compareToIgnoreCase(fieldValue2);
		}
		else{
			return fieldValue2.compareToIgnoreCase(fieldValue1);
		}
	}
	
}