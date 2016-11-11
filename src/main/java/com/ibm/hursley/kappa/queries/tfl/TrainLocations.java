package com.ibm.hursley.kappa.queries.tfl;

import java.util.HashMap;
import java.util.Iterator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.ibm.hursley.kappa.kafka.KappaQuery;

public class TrainLocations extends KappaQuery{
	
	private final Logger logger = Logger.getLogger(TrainLocations.class);
	
	public TrainLocations(String query, String filter){
		super(query,filter);
	}
	
	public void run() {
		this.running = true;
		logger.log(Level.INFO, "Running TrainLocation query");
	
		HashMap<String, Integer> trainLocationCounts = new HashMap<>();
		
		while(this.running){
			Iterator<ConsumerRecord<String, byte[]>> it = this.kafkaConsumer.poll(10000).iterator();
			while (it.hasNext()) {
				ConsumerRecord<String, byte[]> record = it.next();
				String valueString  = new String(record.value());
				if(valueString != null){
					try{
						JSONObject valueJson = new JSONObject(valueString);
						if(valueJson != null){
							if(valueJson.has("currentLocation") && valueJson.getString("currentLocation")!= null && valueJson.getString("currentLocation").length() > 0){
								int currentCount = 0;
								if(trainLocationCounts.containsKey(valueJson.getString("currentLocation"))){
									currentCount = (trainLocationCounts.get(valueJson.getString("currentLocation"))).intValue();
								}
								currentCount++;
								trainLocationCounts.put(valueJson.getString("currentLocation"), new Integer(currentCount));
							}
						}
					}
					catch(Exception e){
						logger.log(Level.ERROR, "Exception parsing " + e.getMessage());
						//logger.log(Level.INFO, valueString);
					}
				}
			}
			
			System.out.println("total counts:" + trainLocationCounts.size());
			this.updateResult(trainLocationCounts);
			this.kafkaConsumer.commitSync();
		}
		
		kafkaConsumer.close();
		logger.log(Level.INFO,"shutting down kafka consumer");
	}
	
	
	public String getResult(){
		JSONArray locations = new JSONArray();
		
		if(this.result instanceof HashMap){
			HashMap<String, Integer> locationsHashmap = (HashMap<String, Integer>) this.result;
			Iterator<String> keysIterator =  locationsHashmap.keySet().iterator();
			while(keysIterator.hasNext()){
				String key = keysIterator.next();
				JSONObject location = new JSONObject();
				location.put("location", key);
				location.put("count",locationsHashmap.get(key).intValue());
				locations.put(location);
			}
		}
		
		
		return locations.toString();
	}
	
	
	
	
	
	
}
