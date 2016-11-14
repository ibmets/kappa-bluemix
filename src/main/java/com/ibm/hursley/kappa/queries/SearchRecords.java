package com.ibm.hursley.kappa.queries;

import java.util.ArrayList;
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
		
		int resultsSize = 10;
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
					
				if(results.size() >= resultsSize){
					results.subList(0, results.size()-resultsSize).clear();
				}
					
				this.updateResult(results);	
			}
			this.kafkaConsumer.commitSync();
			logger.log(Level.INFO, "running, kafka search, results: " + results.size() + " (max:"+resultsSize+")");
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
				results.put(i.next());
				
			}
			result = results.toString(1);
		}
		return result;
	}
	
	
	
	
	
	
	
}
