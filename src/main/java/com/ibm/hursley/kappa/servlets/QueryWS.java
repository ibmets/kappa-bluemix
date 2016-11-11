package com.ibm.hursley.kappa.servlets;

import java.io.IOException;

import javax.websocket.CloseReason;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnMessage;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.PathParam;
import javax.websocket.server.ServerEndpoint;

import com.ibm.hursley.kappa.kafka.KappaListenerInterface;
import com.ibm.hursley.kappa.kafka.KappaQueries;
import com.ibm.hursley.kappa.kafka.KappaQuery;



@ServerEndpoint(value = "/ws/query/{queryhash}")
public class QueryWS {
	
	
	private KappaQueries kappaQueries = null;
	
	public QueryWS() {
		kappaQueries = new KappaQueries();
	}
	
	@OnOpen
	public void open(final Session session, @PathParam("queryhash") String queryHash) {
		System.out.println("ws open:" + queryHash + " session: " + session.getId());
		
		KappaQuery kappaQuery = kappaQueries.getQuery(queryHash);
		
		if(kappaQuery != null){
			
			kappaQuery.addListener(new KappaListenerInterface() {
				
				@Override
				public void updateResult(String data) {
					try {
						if(session != null && session.isOpen()){
							session.getBasicRemote().sendText(data);
						}
					} 
					catch (IOException e) {
						e.printStackTrace();
					}
				}
			},session.getId(), true);
		}
		else{
		
		}
		
	}
	
	@OnMessage
	public String message(String message, Session session, @PathParam("queryhash") String queryHash) {
		System.out.println("ws: message: " + queryHash);
		
		KappaQuery kappaQuery = kappaQueries.getQuery(queryHash);
		if(kappaQuery != null){
			return kappaQuery.getResult();
		}
		else{
			return "0";
		}
		
		
		
	}
	
	@OnClose
	public void close(Session session, CloseReason reason, @PathParam("queryhash") String queryHash) {
		System.out.println("ws: close session: " + session.getId() + " queryHash:" + queryHash);
		if(queryHash != null && queryHash.length() > 0){
			KappaQuery kappaQuery = kappaQueries.getQuery(queryHash);
			if(kappaQuery != null){
				kappaQuery.removeListener(session.getId());
			}
		}
	}
	
	@OnError
	public void error(Throwable t) {
		t.printStackTrace();
	}

}




