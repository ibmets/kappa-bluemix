package com.ibm.hursley.kappa.queries;

import java.text.SimpleDateFormat;
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
					if(item != null && item.has(this.sortField) && item.getString(this.sortField).length() > 0){
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
		if((o1 == null || !o1.has(sortField) || o1.getString(sortField).length() < 1) && (o2 == null || !o2.has(sortField) || o2.getString(sortField).length() < 1)){
			return 0;
		}
		if(o1 == null || !o1.has(sortField) || o1.getString(sortField).length() < 1){
			return -1;
		}
		if(o2 == null || !o2.has(sortField) || o2.getString(sortField).length() < 1){
			return 1;
		}
		
		String fieldValue1 = o1.getString(sortField);
		String fieldValue2 = o2.getString(sortField);
		
		Date dateField1 = parseAsDate(fieldValue1);
		Date dateField2 = parseAsDate(fieldValue2);
		
		
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
			Parser parser = new Parser();
			List<DateGroup> groups = parser.parse(value);
			if(groups.size() == 1){
				if(groups.get(0).getDates().size() > 0){
					return groups.get(0).getDates().get(0);
				}
			}
		}
		return null;
	}
	
}