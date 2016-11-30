package com.ibm.hursley.kappa.queries;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.ibm.hursley.kappa.kafka.KappaQuery;

public class SearchRecords extends KappaQuery{
	
	private final Logger logger = Logger.getLogger(SearchRecords.class);
	
	
	public SearchRecords(String query, String filter){
		super(query,filter);
	}
	
	public void run() {
		this.running = true;
		logger.log(Level.INFO, "Running SearchRecords query");
		
		// setup sorter
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
		}
	
	
		ArrayList<JSONObject> results = new ArrayList<>();
		
		while(this.running){
			Iterator<ConsumerRecord<String, byte[]>> it = this.kafkaConsumer.poll(10000).iterator();
			while (it.hasNext()) {
				ConsumerRecord<String, byte[]> record = it.next();
				if(filterJson != null){
					if(isMatch(record)){
						String valueString  = new String(record.value());
						if(valueString != null){
							try{
								JSONObject valueJson = new JSONObject(valueString);
								results.add(valueJson);
							}
							catch(Exception e){
							}
						}
					}
				}
				else{
					String valueString  = new String(record.value());
					try{
						JSONObject valueJson = new JSONObject(valueString);
						results.add(valueJson);
					}
					catch(Exception e){
					}
				}
			}
			
			if(searchComparator != null){
				results = searchComparator.filterList(results);
				Collections.sort(results,searchComparator);
			}
			
			results = this.trimResults(results, sortLimit);	
			this.updateResult(results);	
			
			this.kafkaConsumer.commitSync();
			logger.log(Level.INFO, "running, kafka search, results: " + results.size());
		}
		
		kafkaConsumer.close();
		logger.log(Level.INFO,"shutting down kafka consumer");
		
	}
	
	
	
	public String getResult(){
		String result = "[]";
		if(this.result instanceof ArrayList){
			JSONArray results = new JSONArray();
			Iterator<JSONObject> i= ((ArrayList<JSONObject>) this.result).iterator();
			while(i.hasNext()){
				
				if(filterJson.has("_source")){
					System.out.println("source filtering");
					JSONObject resultObject = i.next();
					JSONArray sources = filterJson.optJSONArray("_source");
					if(sources != null){
						JSONObject filterSouceObject = new JSONObject();
						for(int j=0; j < sources.length(); j++){
							if(resultObject.has(sources.getString(j))){
								filterSouceObject.put(sources.getString(j), resultObject.get(sources.getString(j)));
							}
						}
						results.put(filterSouceObject);
					}
					else{
						results.put(i.next());
					}
					
				}
				else{
					results.put(i.next());
				}
				
			}
			result = results.toString(1);
		}
		return result;
	}
	
	
	
}
