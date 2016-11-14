package com.ibm.hursley.kappa.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.ibm.hursley.kappa.bluemix.Bluemix;


public class KappaQuery extends Thread{
	
	private final Logger logger = Logger.getLogger(KappaQuery.class);
	
	private String hash = null;
	protected boolean running = false;
	protected KafkaConsumer<String, byte[]> kafkaConsumer = null;
	protected Object result = null;
	protected String query = null;
	protected String filter = null;
	protected JSONObject filterJson = null;
	
	private HashMap<String, KappaListenerInterface> listeners = new HashMap<String, KappaListenerInterface>();
	

	public KappaQuery(String query, String filter){
		this.setQuery(query, filter);
	}

	public String getHash(){
		return this.hash;
	}
	
	public void setQuery(String query, String filter){
		this.query = query;
		this.filter = filter;
		this.hash = KappaQuery.generateHash(query, filter);
		
		if(filter != null){
			try{
				filterJson = new JSONObject(filter);
			}
			catch(Exception e){
				logger.log(Level.ERROR, "Unable to parse filter: " + filter + " " + e.getMessage());
			}
		}
	}
		
	
	public static String generateHash(String query, String filter){
		String hash = "";
		if(query != null){
			hash = query.hashCode() + "";
		}
		if(filter != null){
			hash = hash + filter.hashCode();
		}
		return hash;
	}
	
	
	public void initKafka(){
		this.running = true;
		
		// create kafka consumer
		Properties consumerProperties = (Properties) Bluemix.getConsumerConfiguration().clone();
		consumerProperties.setProperty("client.id", KappaQueries.getClientId());
		consumerProperties.setProperty("group.id", "kappa-bluemix-"+KappaQueries.getClientId()+"-"+this.hash);
		logger.log(Level.INFO, "creating new consumer with ID: " + consumerProperties.getProperty("client.id") + " in group: " + consumerProperties.getProperty("group.id"));
		
		try{
			this.kafkaConsumer = new KafkaConsumer<>(consumerProperties);
		}
		catch(Exception e){
			// beanserver logging can create an aleready exists exception
		}
		
		// set topics
		ArrayList<String> topicList = new ArrayList<String>();
		topicList.add("search");
		this.kafkaConsumer.subscribe(topicList);
		
		// rest to start of stream
		this.resetStream();
	}

		
	public void shutdown(){
		logger.log(Level.INFO, "Shutting down query: " + this.getHash());
		this.running = false;
	}
	
	
	private void resetStream(){
		logger.log(Level.INFO,"Resetting Kafka Stream");
		try{
			kafkaConsumer.poll(10000);
			
			ArrayList<TopicPartition> topicPartions = new ArrayList<>();
			List<PartitionInfo> partitionsInfo =  kafkaConsumer.partitionsFor("search");
			if(partitionsInfo != null){
				Iterator<PartitionInfo> i = partitionsInfo.iterator();
				while(i.hasNext()){
					PartitionInfo partitionInfo = i.next();
					logger.log(Level.INFO,"Resetting partition: " + partitionInfo.topic() + ":" + partitionInfo.partition());
					topicPartions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
				}
			}
			
		    kafkaConsumer.seekToBeginning(topicPartions);
		    kafkaConsumer.commitSync();
		    logger.log(Level.INFO,"Stream Reset Done");
		    
		}
		catch(Exception e){
			logger.log(Level.ERROR, "Unable to reset stream: " + e.getMessage());
		}
	}
	
	
	@Override
	public void run() {

		logger.log(Level.INFO, "Running query");
		int messageCount = 0;
		while(this.running){
			Iterator<ConsumerRecord<String, byte[]>> it = this.kafkaConsumer.poll(10000).iterator();
			while (it.hasNext()) {
				messageCount++;
				this.updateResult(new Integer(messageCount));
				ConsumerRecord<String, byte[]> record = it.next();
				//String message = new String(record.value(), Charset.forName("UTF-8"));
			}
			this.kafkaConsumer.commitSync();
			logger.log(Level.INFO, "running, kafka count: " + messageCount);
		}
		
		kafkaConsumer.close();
		logger.log(Level.INFO,"shutting down kafka consumer");
		
	}
	
	
	protected void updateResult(Object result){
		this.result = result;
		Iterator<String> keys = this.listeners.keySet().iterator();
		while(keys.hasNext()){
			KappaListenerInterface listener = this.listeners.get(keys.next());
			if(listener != null){
				listener.updateResult(getResult());
			}
		}
	}
	
	
	public String getResult(){
		String result = "";
		if(this.result instanceof Integer){
			result = ((Integer) this.result).intValue() + "";
		}
		
		return result;
	}
	
	
	public void addListener(KappaListenerInterface kappaListener, String sessionId, boolean sendInitial){
		this.listeners.put(sessionId, kappaListener);
		if(sendInitial){
			kappaListener.updateResult(getResult());
		}
	}
	
	public void removeListener(String sessionId){
		this.listeners.remove(sessionId);
		if(this.listeners.size() < 1){
			logger.log(Level.INFO, "Query " + this.getHash() + " has no more listeners, stopping and removing");
			KappaQueries.removeQuery(this.getHash());	
		}
		
	}

	public boolean isRunning() {
		return running;
	}
	
	
	protected boolean isMatch(ConsumerRecord<String, byte[]> record){
		boolean match = true;
		
		String valueString  = new String(record.value());
		if(valueString != null){
			try{
				JSONObject valueJson = new JSONObject(valueString);
				if(valueJson != null){
					if(filterJson.has("match")){
						JSONObject matchJson = filterJson.getJSONObject("match");
						Iterator<String> matchFields = matchJson.keys();
						while(matchFields.hasNext()){
							String matchField = matchFields.next();
							if(valueJson.has(matchField) && valueJson.getString(matchField).equalsIgnoreCase(matchJson.getString(matchField))){
								
							}
							else{
								return false;
							}
						}
					}
				}
				else{
					match = false;
				}
			}
			catch(Exception e){
				match = false;
			}
		}
		else{
			match = false;
		}
		
		return match;
	}
	
	
}
