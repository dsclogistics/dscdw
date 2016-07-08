package com.dsc.dw.internal;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.ws.rs.core.Response;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.dsc.dw.dao.ConnectionManager;

import java.io.*;
import java.net.*;

public class ThroughPutLoad {
	
		
	public Response ThroughPutLoad(JSONObject inputJsonObj) throws JSONException {
		 Response rb = null;
		 String  msg = null;
		String theurl="";
		String dwurl="";
		String query="";
		String tmperiodid=null;
		String dscmtrclcbldid=null;
		String mtrcperiodid=null;
		
		JSONObject obj1 = new JSONObject();
		JSONObject api = new JSONObject();
     	JSONObject md =  new  JSONObject();  
      	JSONArray results = new JSONArray();
		
		 
		try {
		    Context ctx = new InitialContext();
		    ctx = (Context) ctx.lookup("java:comp/env");
		    theurl = (String) ctx.lookup("mtrcurl");
		}
		catch (NamingException e) {
	          // msg[0]="-1";
	           msg= "{\"result\":\"FAILED\",\"resultCode\":500,\"message\":\""  +
	        		   "Cannot access/find mtrcurl in context.xml"  +"\"}";
	           rb=Response.ok(msg.toString()).build();
		}	
		
		try {
		    Context ctx = new InitialContext();
		    ctx = (Context) ctx.lookup("java:comp/env");
		    dwurl = (String) ctx.lookup("dwurl");
		}
		catch (NamingException e) {
	          // msg[0]="-1";
	           msg= "{\"result\":\"FAILED\",\"resultCode\":500,\"message\":\""  +
	        		   "Cannot access/find dwurl in context.xml"  +"\"}";
	           rb=Response.ok(msg.toString()).build();
		}	
	  
		 // ========================================================= 	     
	     // first Call Time Period API to get Time period
	     URL url = null;
		try {
			url = new URL(theurl + "metrictimeperiod");
		} catch (MalformedURLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	     query =  "{ 'tptname':'Month','calmonth':'May','calyear':2016}";
	     // result will be:"tm_period_id":"4","tpt_id":"6"
	     //make connection
	        URLConnection urlc = null;
			try {
				urlc = url.openConnection();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
	        urlc.setRequestProperty("Content-Type","application/json");
	        // urlc.setRequestProperty("Accept", "application/json");

	        //use post mode
	        urlc.setDoOutput(true);
	        urlc.setAllowUserInteraction(false);

	        //send query
	        PrintStream ps = null;

			try {
				ps = new PrintStream(urlc.getOutputStream());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
	        ps.print(query);
	        ps.close();

	        //get result
	        BufferedReader br = null;
	       JSONObject xx = new JSONObject();
	       // JsonReader jsonReader = Json.createReader(fis);
			try {
				br = new BufferedReader(new InputStreamReader(urlc.getInputStream()));
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			 
			System.out.println("buffer read after the call"+br.toString());
			//  xx = new JSONObject(br.toString());
		//	System.out.println(xx);
			
			String data=null;
	        String l = null;
	        StringBuilder responseStrBuilder = new StringBuilder();

	        try {
				while ((l=br.readLine())!=null) {
					data=data+l;
					responseStrBuilder.append(l);
				    System.out.println(l);
				}
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
	        try {
				br.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}		
 

	     catch (Exception e) {
	         e.printStackTrace();
	       }
	        
	        
	        
	        
	//        
	         if (data != null) 
	        	 {
	        	  api = new JSONObject(responseStrBuilder.toString());
	        	  
	        	// JSON japi = new JSON(jsonstring.toString());
	        	 }
 
	         if(api.has("tm_period_id")) 
	        	 {
	        	 tmperiodid=api.get("tm_period_id").toString();    
	        	 }
	         else
	         {
		         //  msg[0]="-1";
		           msg= "{\"result\":\"FAILED\",\"resultCode\":500,\"message\":\""  +
		        		   "metrictimeperiod api failed to retrun Time Period ID"  +"\"}";
		         // return msg; 
	         }
	        
	 // =========================================================       
	        
	 // Calling WMS Building 
			 // ========================================================= 	     
		     //  Call WMS Building API to get lcbuidling id
		    url = null;
			try {
				url = new URL(theurl + "wmsbuilding");
			} catch (MalformedURLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		     query =  " {'building':'BP2'}";
		     // result will be:  {"building":"BP2"}
		     //make connection
		      urlc = null;
				try {
					urlc = url.openConnection();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
		        urlc.setRequestProperty("Content-Type","application/json");
		        //use post mode
		        urlc.setDoOutput(true);
		        urlc.setAllowUserInteraction(false);

		        //send query
		          ps = null;
				try {
					ps = new PrintStream(urlc.getOutputStream());
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
		        ps.print(query);
		        ps.close();

		        //get result
		          br = null;
				try {
					br = new BufferedReader(new InputStreamReader(urlc.getInputStream()));
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				  data=null;
		          l = null;
		          StringBuilder responseStrBuildera = new StringBuilder();



			        try {
						while ((l=br.readLine())!=null) {
							data=data+l;
							responseStrBuildera.append(l);
						    System.out.println(l);
						}
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
		        try {
					br.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}		
		
		        if (data != null)     api = new JSONObject(responseStrBuildera.toString());
	 
		         if(api.has("dsc_mtrc_lc_bldg_id")) 
		        	 {
		        	 dscmtrclcbldid=api.get("dsc_mtrc_lc_bldg_id").toString();    
		        	 }
		         else
		         {
			         //  msg[0]="-1";
			           msg= "{\"result\":\"FAILED\",\"resultCode\":500,\"message\":\""  +
			        		   "wmsbuilding api failed to retrun WMS LC BLD ID"  +"\"}";
			         // return msg; 
		         }
		        
		 // =========================================================  
		         
		 // call metricname to get metric period id
         //========================================== 	     
 
 
		         Calendar cal =  Calendar.getInstance();
		         cal.add(Calendar.MONTH ,-1);
		         //format it to MMM-yyyy // January-2012
		         String previousMonth  = new SimpleDateFormat("MMM").format(cal.getTime());
		         String thisyear  = new SimpleDateFormat("yyyy").format(cal.getTime());
 
		         
		    		    url = null;
		    			try {
		    				url = new URL(theurl + "metricname");
		    			} catch (MalformedURLException e1) {
		    				// TODO Auto-generated catch block
		    				e1.printStackTrace();
		    			}
		    		     query =  " {\"productname\":\"Red Zone\", \"tptname\":\"Month\",\"calmonth\":\"" +
		    			           previousMonth +"\",\"calyear\":"+thisyear +",\"metricname\":\"Throughput Chg %\"}";
		    		    System.out.println("query is:"+query);
		    		     // result will be:  {
		    		   //  "metricdetail": {
		    		   // 	    "tptname": "Month",
		    		   // 	    "productname": "Red Zone"
		    		   // 	  },
		    		   // 	  "metriclist": [
		    		   // 	    {
		    		   // 	      "mtrc_name": "Throughput Chg %",
		    		   // 	      "mtrc_id": "18",
		    		   // 	      "mtrc_period_id": "6"
		    		   // 	    }
		    		   // 	  ]
		    		   // 	}
		    		     //make connection
		    		      urlc = null;
		    				try {
		    					urlc = url.openConnection();
		    				} catch (IOException e1) {
		    					// TODO Auto-generated catch block
		    					e1.printStackTrace();
		    				}
		    		        urlc.setRequestProperty("Content-Type","application/json");
		    		        //use post mode
		    		        urlc.setDoOutput(true);
		    		        urlc.setAllowUserInteraction(false);

		    		        //send query
		    		          ps = null;
		    				try {
		    					ps = new PrintStream(urlc.getOutputStream());
		    				} catch (IOException e1) {
		    					// TODO Auto-generated catch block
		    					e1.printStackTrace();
		    				}
		    		        ps.print(query);
		    		        ps.close();

		    		        //get result
		    		          br = null;
		    				try {
		    					br = new BufferedReader(new InputStreamReader(urlc.getInputStream()));
		    				} catch (IOException e1) {
		    					// TODO Auto-generated catch block
		    					e1.printStackTrace();
		    				}
		    				  data=null;
		    		          l = null;
		    		            responseStrBuildera = new StringBuilder();



		    			        try {
		    						while ((l=br.readLine())!=null) {
		    							data=data+l;
		    							responseStrBuildera.append(l);
		    						    System.out.println(l);
		    						}
		    					} catch (IOException e1) {
		    						// TODO Auto-generated catch block
		    						e1.printStackTrace();
		    					}
		    		        try {
		    					br.close();
		    				} catch (IOException e1) {
		    					// TODO Auto-generated catch block
		    					e1.printStackTrace();
		    				}		
		    		
		    		        if (data != null)     api = new JSONObject(responseStrBuildera.toString());
		    	 
		    		         if(api.has("metriclist")) 
		    		        	 {
		    		         // md =  (JSONObject) api.get("metricdetail"); 
		    		          results = api.getJSONArray("DSCWMSVolumes");
		    		        	    
		    		        	 }
		    		         else
		    		         {
		    			         //  msg[0]="-1";
		    			           msg= "{\"result\":\"FAILED\",\"resultCode\":500,\"message\":\""  +
		    			        		   "wmsbuilding api failed to retrun WMS LC BLD ID"  +"\"}";
		    			         // return msg; 
		    			           rb=Response.ok(msg.toString()).build();
		    		         }
		    		        
		    		 // =========================================================  		         

    obj1.put("tptid",tmperiodid);   
    obj1.put("wmsbld",dscmtrclcbldid); 
    for (int i=0; i<results.length(); i++) 
 	{
 		JSONObject first = results.getJSONObject(i);
 		if (first.has("mtrc_period_id")){
 			 obj1.put("mtrcpid", first.get("mtrc_period_id").toString());
 			mtrcperiodid=first.get("mtrc_period_id").toString();}
 			// System.out.println("mtrci period id is:"+first.get("mtrc_period_id").toString());}
 
 	}
    
    // if you are here then all good now call DW api and load the data
	 // =========================================================  
    
	 // call metricname to get metric period id
    //========================================== 	     


	         cal =  Calendar.getInstance();
	         cal.add(Calendar.MONTH ,-1);
	         //format it to MMM-yyyy // January-2012
	          previousMonth  = new SimpleDateFormat("MMM").format(cal.getTime());
	          thisyear  = new SimpleDateFormat("yyyy").format(cal.getTime());

	         
	    		    url = null;
	    			try {
	    				url = new URL(theurl + "dscwmsvolume");
	    			} catch (MalformedURLException e1) {
	    				// TODO Auto-generated catch block
	    				e1.printStackTrace();
	    			}
	    			// {"productname":"Red Zone", "tptname":"Month","mtrcid":3,"calmonth":"May","calyear":2016}
	    		     query =  " {\"productname\":\"Red Zone\", \"tptname\":\"Month\",\"calmonth\":\"" +
	    			           previousMonth +"\",\"calyear\":"+thisyear +"\"";
	    		    System.out.println("query is:"+query);
	    		     // result will be:  DSCWMSVolumes Jsonarray
	    		//    {
	    		//    	"LC_CODE": "BP"
	    		//    	"PRIMARY_BUILDING_ID": "2"
	    		//    	"MONTH_YEAR": "2016 05"
	    		//    	"LCBLD": "BP2"
	    	//	    	"YYMM": "201605"
	    	//	    	"THROUGHPUT_PCT_CHG": "-0.027618"
	    	//	    	}
 	    		     //make connection

	    		    
	    		     urlc = null;
	    				try {
	    					urlc = url.openConnection();
	    				} catch (IOException e1) {
	    					// TODO Auto-generated catch block
	    					e1.printStackTrace();
	    				}
	    		        urlc.setRequestProperty("Content-Type","application/json");
	    		        //use post mode
	    		        urlc.setDoOutput(true);
	    		        urlc.setAllowUserInteraction(false);

	    		        //send query
	    		          ps = null;
	    				try {
	    					ps = new PrintStream(urlc.getOutputStream());
	    				} catch (IOException e1) {
	    					// TODO Auto-generated catch block
	    					e1.printStackTrace();
	    				}
	    		        ps.print(query);
	    		        ps.close();

	    		        //get result
	    		          br = null;
	    				try {
	    					br = new BufferedReader(new InputStreamReader(urlc.getInputStream()));
	    				} catch (IOException e1) {
	    					// TODO Auto-generated catch block
	    					e1.printStackTrace();
	    				}
	    				  data=null;
	    		          l = null;
	    		            responseStrBuildera = new StringBuilder();



	    			        try {
	    						while ((l=br.readLine())!=null) {
	    							data=data+l;
	    							responseStrBuildera.append(l);
	    						    System.out.println(l);
	    						}
	    					} catch (IOException e1) {
	    						// TODO Auto-generated catch block
	    						e1.printStackTrace();
	    					}
	    		        try {
	    					br.close();
	    				} catch (IOException e1) {
	    					// TODO Auto-generated catch block
	    					e1.printStackTrace();
	    				}		
	    		
	    		        if (data != null)     api = new JSONObject(responseStrBuildera.toString());
	    	 
	    		         if(api.has("metriclist")) 
	    		        	 {
	    		          md =  (JSONObject) api.get("metricdetail"); 
	    		          results = api.getJSONArray("metriclist");
	    		        	    
	    		        	 }
	    		         else
	    		         {
	    			         //  msg[0]="-1";
	    			           msg= "{\"result\":\"FAILED\",\"resultCode\":500,\"message\":\""  +
	    			        		   "wmsbuilding api failed to retrun WMS LC BLD ID"  +"\"}";
	    			         // return msg; 
	    			           rb=Response.ok(msg.toString()).build();
	    		         }
	    		        
	    		 // =========================================================   
     
	//  msg="'tptid':'"+tmperiodid.toString()+"','wmsbld':'"+dscmtrclcbldid.toString()+"'";
	  rb=Response.ok(obj1.toString()).build();
  	return rb;
}
}

 
