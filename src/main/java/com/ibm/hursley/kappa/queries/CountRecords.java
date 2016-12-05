package com.ibm.hursley.kappa.queries;

import java.util.Iterator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.ibm.hursley.kappa.kafka.KappaQuery;

public class CountRecords extends KappaQuery{
	
	private final Logger logger = Logger.getLogger(CountRecords.class);
	
	public CountRecords(String query, String filter){
		super(query,filter);
	}
	
	public void run() {
		this.running = true;
		logger.log(Level.INFO, "Running CountRecords query");
		int messageCount = 0;
		while(this.running){
			Iterator<ConsumerRecord<String, byte[]>> it = this.kafkaConsumer.poll(10000).iterator();
			while (it.hasNext()) {
				ConsumerRecord<String, byte[]> record = it.next();
				if(filterJson != null){
					if(isMatch(record)){
						messageCount++;
					}
				}
				else{
					messageCount++;
				}
				this.updateResult(new Integer(messageCount));	
			}
			this.kafkaConsumer.commitSync();
			logger.log(Level.INFO, "running, kafka count: " + messageCount);
		}
		
		kafkaConsumer.close();
		logger.log(Level.INFO,"shutting down kafka consumer");
		
	}
	
	
	public String getResult(){
		JSONObject jsonResult = new JSONObject();
		if(this.result instanceof Integer){
			jsonResult.put("count",((Integer) this.result).intValue());
		}
		return jsonResult.toString();
	}
	
	
	
	
	
	
}
