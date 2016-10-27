package com.ibm.hursley.kappa.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.ibm.hursley.kappa.bluemix.Bluemix;

public class Producer {
	
	private KafkaProducer<String, byte[]> kafkaProducer = null;
	
	
	public Producer(){
	}
	
	
	public KafkaProducer<String, byte[]> getProducer(){
		if(this.kafkaProducer == null){
			this.kafkaProducer = new KafkaProducer<>(Bluemix.getProducerConfiguration());
		}	
		return this.kafkaProducer;
	}
	
	
	public void addMessage(String message){
		KafkaProducer<String, byte[]> kafkaProducer = getProducer();
		try{
			RecordMetadata meta = kafkaProducer.send(new ProducerRecord<String, byte[]>("search", "hello".getBytes())).get();
			System.out.println(meta.topic() + " " + meta.offset() + " " + meta.partition());
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
}
