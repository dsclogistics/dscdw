package com.dsc.dw.rest;



//here is where I added ldap stuff
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchResult;
//import javax.servlet.ServletContext;
import javax.naming.NamingEnumeration;
import javax.naming.AuthenticationException;
import javax.naming.AuthenticationNotSupportedException;
import javax.naming.Context;
import javax.naming.NameClassPair;
//import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.*;
//import org.apache.http.HttpEntity;
//import org.apache.http.util.EntityUtils;

import java.sql.Timestamp;
import java.util.Hashtable; 
//ending lDAP stuff

//new import for json

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONObject;

import com.dsc.dw.internal.*; 

@Path("/v1/dw") 
public class DW {

@GET
	@Produces(MediaType.TEXT_HTML)
	public String returnTitle()
	{
	//ServletContext sc = getServletContext();
	//String testNameValue = sc.getInitParameter("testName");
	java.util.Date date= new java.util.Date();
	/* 
	APIEvent R1 = new APIEvent( "Thread-1");
   R1.start();
  	java.util.Date date= new java.util.Date();
	 	System.out.println(" Return back to user at "+new Timestamp(date.getTime()));
	 	 */
		return "<p>Default Data Warehouse Service</p>"+new Timestamp(date.getTime());
	}

//****************  Authenication Service
@Path("/whoami")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response whoami(JSONObject inputJsonObj) throws Exception {
	  
	java.util.Date date= new java.util.Date();
	java.util.Date sdate=new Timestamp(date.getTime());
	  
	 Response rb = null;
	 ldap vr = new ldap();
	 rb=vr.ldap(inputJsonObj);
 
 
	     return rb;
	  
	}

//**************** DSC WMS Volume
@Path("/dscwmsvolume")
@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response DSCWMSVolume(JSONObject inputJsonObj) throws Exception {
	 Response rb = null;	 
	java.util.Date date= new java.util.Date();
	java.util.Date sdate=new Timestamp(date.getTime());  
	 DSCWMSVolume dscwmsvolume = new DSCWMSVolume();
	  rb=dscwmsvolume.DSCWMSVolume(inputJsonObj);
	     return rb;   
	  
	}
}
