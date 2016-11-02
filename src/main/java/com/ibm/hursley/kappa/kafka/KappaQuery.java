package com.ibm.hursley.kappa.kafka;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.ibm.hursley.kappa.bluemix.Bluemix;


public class KappaQuery extends Thread{
	


	private final Logger logger = Logger.getLogger(KappaQuery.class);
	
	private String hash = null;
	private boolean running = false;
	private KafkaConsumer<String, byte[]> kafkaConsumer = null;
	
	private ArrayList<KappaListenerInterface> listeners = new ArrayList<>();
	
	private Object result = null;

	
	public KappaQuery(){
		
	}
	
	public KappaQuery(String query){
		this.hash = KappaQuery.generateHash(query);
	}

	public String getHash(){
		return this.hash;
	}
	
	public void setQuery(String query){
		this.hash = KappaQuery.generateHash(query);
	}
	
	
	
	
	public static String generateHash(String query){
		String hash = null;
		if(query != null){
			hash = query.hashCode() + "";
		}
		return hash;
	}
	
	
	public void initKafka(){
		
		// create kafka consumer
		Properties consumerProperties = (Properties) Bluemix.getConsumerConfiguration().clone();
		consumerProperties.setProperty("client.id", KappaQueries.getClientId());
		consumerProperties.setProperty("group.id", "kappa-bluemix-"+this.hash);
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
		logger.log(Level.INFO,"Resetting Kafka stream");
		kafkaConsumer.poll(1000);
	    ArrayList<TopicPartition> topicPartions = new ArrayList<>();
	    topicPartions.add(new TopicPartition("search",0));
	    kafkaConsumer.seekToBeginning(topicPartions);
	    kafkaConsumer.commitSync();
	}
	
	
	@Override
	public void run() {
		this.running = true;
		logger.log(Level.INFO, "Running query");
		int messageCount = 0;
		int iteration = 0;
		while(this.running){
			System.out.println("running query iteration: " + iteration);
			iteration++;
			Iterator<ConsumerRecord<String, byte[]>> it = this.kafkaConsumer.poll(10000).iterator();
			while (it.hasNext()) {
				messageCount++;
				this.updateResult(new Integer(messageCount));
				ConsumerRecord<String, byte[]> record = it.next();
				String message = new String(record.value(), Charset.forName("UTF-8"));
				logger.log(Level.INFO, "Message: " + message.toString());
			}
			this.kafkaConsumer.commitSync();
			logger.log(Level.INFO, "running, kafka count: " + messageCount);
		}
		
		kafkaConsumer.close();
		logger.log(Level.INFO,"shutting down kafka consumer");
		
	}
	
	
	private void updateResult(Object result){
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
