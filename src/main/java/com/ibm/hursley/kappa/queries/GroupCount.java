package com.ibm.hursley.kappa.queries;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.ibm.hursley.kappa.kafka.KappaQuery;
import com.ibm.hursley.kappa.queries.SearchComparator;

public class GroupCount extends KappaQuery{
	
	private final Logger logger = Logger.getLogger(GroupCount.class);
	//private String groupField = "currentLocation";
	private String groupField = null;;
	
	//{"group_by":{"field":"lineName"}
	
	public GroupCount(String query, String filter){
		super(query,filter);
		this.parseGroupField();
	}
	
	private void parseGroupField(){
		if(this.filterJson!=null){
			if(this.filterJson.has("group_by") && this.filterJson.getJSONObject("group_by")!=null){
				JSONObject groupByObject = this.filterJson.getJSONObject("group_by");
				if(groupByObject.has("field") && groupByObject.optString("field").length()>0){
					this.groupField = groupByObject.getString("field");
				}
			}
		}
	}
	
	public void run() {
		this.running = true;
		logger.log(Level.INFO, "Running GroupCount query");
	
		HashMap<String, Integer> groupElementsCounts = new HashMap<>();
		
		if(this.groupField != null && groupField.length() > 0){
			while(this.running){
				Iterator<ConsumerRecord<String, byte[]>> it = this.kafkaConsumer.poll(10000).iterator();
				while (it.hasNext()) {
					ConsumerRecord<String, byte[]> record = it.next();
					String valueString  = new String(record.value());
					if(valueString != null){
						try{
							JSONObject valueJson = new JSONObject(valueString);
							if(valueJson != null){
								if(valueJson.has(groupField) && valueJson.getString(groupField)!= null && valueJson.getString(groupField).length() > 0){
									int currentCount = 0;
									if(groupElementsCounts.containsKey(valueJson.getString(groupField))){
										currentCount = (groupElementsCounts.get(valueJson.getString(groupField))).intValue();
									}
									currentCount++;
									groupElementsCounts.put(valueJson.getString(groupField), new Integer(currentCount));
								}
							}
						}
						catch(Exception e){
							logger.log(Level.ERROR, "Exception parsing " + e.getMessage());
							//logger.log(Level.INFO, valueString);
						}
					}
				}
				
				this.updateResult(groupElementsCounts);
				this.kafkaConsumer.commitSync();
			}
		}
	
		kafkaConsumer.close();
		logger.log(Level.INFO,"shutting down kafka consumer");
	}
	
	
	public String getResult(){	
		// first need to put in to a sortable list
		ArrayList<JSONObject> sortableList = new ArrayList<>();
		if(this.result instanceof HashMap){
			HashMap<String, Integer> locationsHashmap = (HashMap<String, Integer>) this.result;
			Iterator<String> keysIterator =  locationsHashmap.keySet().iterator();
			while(keysIterator.hasNext()){
				String key = keysIterator.next();
				if(this.groupField != null){
					JSONObject result = new JSONObject();
					result.put(this.groupField, key);
					result.put("count",locationsHashmap.get(key).intValue());
					sortableList.add(result);
				}
			}
		}
		
		
		// sort them if they are to be ordered
		String sortKey = null;
		String sortOrder = "asc";
		int sortLimit = 10;
		SearchComparator searchComparator = null;
		if(filterJson != null && filterJson.has("sort") && filterJson.getJSONArray("sort").length() > 0){
			JSONObject sortCriteria = filterJson.getJSONArray("sort").getJSONObject(0);
			Iterator<String> i = sortCriteria.keySet().iterator();
			while(i != null && i.hasNext()){
				sortKey = i.next();
			}
			
			if(sortKey != null && sortCriteria.getJSONObject(sortKey).has("order")){
				sortOrder = sortCriteria.getJSONObject(sortKey).getString("order");
			}
			
			if(sortKey != null && sortCriteria.getJSONObject(sortKey).has("limit")){
				sortLimit = sortCriteria.getJSONObject(sortKey).getInt("limit");
			}
		}
		
		if(sortKey != null){
			searchComparator = new SearchComparator(sortKey,sortOrder);
			sortableList = searchComparator.filterList(sortableList);
			Collections.sort(sortableList,searchComparator);
			sortableList = this.trimResults(sortableList, sortLimit);
		}
	

		// convert to json array
		JSONArray locations = new JSONArray();		
		for(int i=0; i < sortableList.size(); i++){
			locations.put(sortableList.get(i));
		}
		
		
		return locations.toString();
	}
	
	
	
	
	
	
}
