package com.ibm.hursley.kappa.queries;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.json.JSONObject;

import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;

public class SearchComparator implements Comparator<JSONObject>{

	private String sortField = null;
	private String sortOrder = null;
	private static final Parser DATE_PARSER = new Parser();
	
	public SearchComparator(String field, String order) {
		this.sortField = field;
		this.sortOrder = order;
	}
	
	
	public ArrayList<JSONObject> filterList(ArrayList<JSONObject> list){
		ArrayList<JSONObject> filteredList = new ArrayList<>();
		
		if(list != null){
			Iterator<JSONObject> i = list.iterator();
			while(i.hasNext()){
				JSONObject item = i.next();
				if(this.sortField != null && this.sortField.length() > 0){
					if(item != null && item.has(this.sortField) && item.optString(this.sortField).length() > 0){
						filteredList.add(item);
					}
					else if(item != null && item.has(this.sortField) && (item.optInt(this.sortField,Integer.MIN_VALUE) > Integer.MIN_VALUE)){
						filteredList.add(item);
					}
				}
				else{
					filteredList.add(item);
				}
			}
		}
		
		
		return filteredList;
	}
	
	@Override
	public int compare(JSONObject o1, JSONObject o2) {
		
		// handle empty or null values
		if((o1 == null || !o1.has(sortField) || o1.optString(sortField).length() < 1) && (o2 == null || !o2.has(sortField) || o2.optString(sortField).length() < 1)){
			return 0;
		}
		if(o1 == null || !o1.has(sortField) || o1.optString(sortField).length() < 1){
			return -1;
		}
		if(o2 == null || !o2.has(sortField) || o2.optString(sortField).length() < 1){
			return 1;
		}
		
		String fieldValue1 = o1.optString(sortField);
		String fieldValue2 = o2.optString(sortField);
		
		Date dateField1 = parseAsDate(fieldValue1);
		Date dateField2 = parseAsDate(fieldValue2);
		
		Integer intField1 = parseAsInt(fieldValue1);
		Integer intField2 = parseAsInt(fieldValue2);
		
		if(dateField1 != null && dateField2 != null){
			// date strings
			if(dateField1.getTime() == dateField2.getTime()){
				return 0;
			}
			
			if(sortOrder.equalsIgnoreCase("asc")){
				if(dateField1.getTime() < dateField2.getTime()){
					return -1;
				}
				else{
					return 1;
				}
			}
			else{
				if(dateField1.getTime() < dateField2.getTime()){
					return 1;
				}
				else{
					return -1;
				}
			}
		}
		else if(intField1 != null && intField2 != null){
			if(sortOrder.equalsIgnoreCase("asc")){
				return intField1.intValue() - intField2.intValue();
			}
			else{
				return intField2.intValue() - intField1.intValue();
			}
		}
		else{
			// standard strings
			if(sortOrder.equalsIgnoreCase("asc")){
				return fieldValue2.compareToIgnoreCase(fieldValue1);
			}
			else{
				return fieldValue1.compareToIgnoreCase(fieldValue2);
			}
		}
	}
	
	
	public Date parseAsDate(String value){
		value = value.trim();
		if(value.split("\\s+").length == 1){
			
			List<DateGroup> groups = SearchComparator.DATE_PARSER.parse(value);
			if(groups.size() == 1){
				if(groups.get(0).getDates().size() > 0){
					return groups.get(0).getDates().get(0);
				}
			}
		}
		return null;
	}
	
	
	public Integer parseAsInt(String value){
		try{
			int parsedInt = Integer.parseInt(value);
			return new Integer(parsedInt);
		}
		catch(Exception e){
			return null;
		}
	}
	
}