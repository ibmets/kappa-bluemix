package com.ibm.hursley.kappa.servlets;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.ibm.hursley.kappa.bluemix.Bluemix;

public class Startup extends HttpServlet {

	private static final long serialVersionUID = 2925378283441510731L;
	
	private final Logger logger = Logger.getLogger(Startup.class);
	

	@Override
	public void init() throws ServletException {
		super.init();
		logger.log(Level.INFO, "Running initial setup check");
		Bluemix.runInitialSetup();
		
		
	}
	

}
