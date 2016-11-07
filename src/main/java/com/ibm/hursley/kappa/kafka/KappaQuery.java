package com.ibm.hursley.kappa.kafka;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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
	
	private ArrayList<KappaListenerInterface> listeners = new ArrayList<>();
	

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
		
		// create kafka consumer
		Properties consumerProperties = (Properties) Bluemix.getConsumerConfiguration().clone();
		consumerProperties.setProperty("client.id", KappaQueries.getClientId());
		consumerProperties.setProperty("group.id", "kappa-bluemix-"+KappaQueries.getClientId()+"-"+this.hash);
		logger.log(Level.INFO, "creating new consumer with ID: " + consumerProperties.getProperty("client.id") + " in group: " + consumerProperties.getProperty("group.id"));
		this.kafkaConsumer = new KafkaConsumer<>(consumerProperties);
		
		// set topics
		ArrayList<String> topicList = new ArrayList<String>();
		topicList.add("search");
		this.kafkaConsumer.subscribe(topicList);
		
		// rest to start of stream
		this.resetStream();
	}

		
	public void shutdown(){
		this.running = false;
	}
	
	
	private void resetStream(){
		logger.log(Level.INFO,"Resetting Kafka stream A");
		try{
			kafkaConsumer.poll(10000);
		    ArrayList<TopicPartition> topicPartions = new ArrayList<>();
		    topicPartions.add(new TopicPartition("search",0));
		    kafkaConsumer.seekToBeginning(topicPartions);
		    kafkaConsumer.commitSync();
		    logger.log(Level.INFO,"Stream Reset");
		    
		}
		catch(Exception e){
			logger.log(Level.ERROR, "Unable to reset stream: " + e.getMessage());
		}
	}
	
	
	@Override
	public void run() {
		this.running = true;
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
		Iterator<KappaListenerInterface> i = this.listeners.iterator();
		while(i.hasNext()){
			KappaListenerInterface listener = i.next();
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
	
	
	public void addListener(KappaListenerInterface kappaListener, boolean sendInitial){
		this.listeners.add(kappaListener);
		if(sendInitial){
			kappaListener.updateResult(getResult());
		}
	}

	public boolean isRunning() {
		return running;
	}
	
	
}
