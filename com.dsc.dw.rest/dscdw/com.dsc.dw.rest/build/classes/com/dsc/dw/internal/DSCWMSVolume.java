package com.dsc.dw.internal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Set;
import java.sql.ResultSetMetaData;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONArray;
 


import com.dsc.dw.dao.*; 


public class DSCWMSVolume  {
	 
	JSONObject jo = new JSONObject();
    String foundid="N";
    String bldid="";
    String theurl="";
    String dscmtrclcbldid="";
    String wrkdscmtrclcbldid="";
	
	public Response DSCWMSVolume(JSONObject inputJsonObj) throws JSONException {
		
		 Response rb = null;
		StringBuffer sb = new StringBuffer();
		StringBuffer sbn = new StringBuffer();
        JSONArray jsona = new JSONArray();
        JSONObject obj1 = new JSONObject();
        JSONObject jobj = new JSONObject();
        int rcount=0;
        String param="";
        // Get tp 
   
        
 
 
        	Calendar calNow = Calendar.getInstance();
             calNow.set(2016, 7, 01);

        		// adding -1 month
        		calNow.add(Calendar.MONTH, -11);

        // fetching updated time
        	//	Date dateBeforeAMonth = calNow.getTime();
        		System.out.println("Date before a month is:"+calNow.getTime());
 
        
        
        
        
        
     	// if (s1.get("tpt_name").toString().equals("COLLECTED")) 
       	 
         int calyear= Integer.parseInt(inputJsonObj.get("calyear").toString());
         String calmonth=inputJsonObj.get("calmonth").toString();
      	 
         DateMagic dm = new DateMagic ();
         String range=dm.MonthRange(calyear, calmonth, -11);
         String dtrange [] = range.split("and");
         String thismy= dtrange[1].substring(0,9).replace("'", "").trim();; 
         
          System.out.println("First field is:"+dtrange[0] +" Second is:"+dtrange[1]);
         String strtdtm ="01";
         int monthnum=0;
         String wrklcbld="";
         String dcmtrclcbldg="";
         try
         {
         	Date date = new SimpleDateFormat("MMM").parse(calmonth);//put your month name here
          Calendar cal = Calendar.getInstance();
          cal.setTime(date);
           monthnum=cal.get(Calendar.MONTH)+1;
           // if (monthnum == 11) monthnum++;
  
         	    NumberFormat f = new DecimalFormat("00");
         	     strtdtm = String.valueOf(f.format(monthnum));
               // strtdtm = calyear + strtdtm + "01";
         }
         catch(Exception e)
         {
         	   System.out.println("Calendar month sent:"+calmonth +" Number is not there"); 
         }
        	 
			 Connection conn = null;
 				try {
					conn= ConnectionManager.mtrcConn().getConnection();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					//e.printStackTrace();
	                String msg="DataWarehouse DB Connection Failed.";
	                sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
	   	          rb=Response.ok(sb.toString()).build();
	   	          return rb;
				}
  
		 try {
 
			 String SQL="SELECT LC_CODE,CASE WHEN(PRIMARY_BUILDING_ID = '' or PRIMARY_BUILDING_ID IS NULL) "+
			 " THEN 'UNKNOWN' ELSE PRIMARY_BUILDING_ID END AS PRIMARY_BUILDING_ID, "+
			 " LCBLD,MONTH_YEAR,(sum(TOTAL_CASES_SHIPPED) + sum(TOTAL_CASES_RECEIVED)) / count(LC_CODE) as THROUGHPUT "+
			 " FROM ( SELECT LC.DSC_LC_CD AS LC_CODE,OB.PRIMARY_BLDG_ID AS PRIMARY_BUILDING_ID, "+
			 " LC.DSC_LC_CD   + CASE WHEN(	OB.PRIMARY_BLDG_ID = '' or 	OB.PRIMARY_BLDG_ID IS NULL) " +
			 " THEN 'UNKNOWN' ELSE OB.PRIMARY_BLDG_ID END    as LCBLD, 	DATES.DT_CAL_MONTH_YEAR_CD AS MONTH_YEAR, "+
			 " SUM(ORDERS.CASES_SHIPPED_QTY) AS TOTAL_CASES_SHIPPED,  0 AS TOTAL_CASES_RECEIVED "+
			 " FROM DSC_DW_MSTR.DBO.F_OB_ORDER_TIMELINE ORDERS "+
			 " LEFT OUTER JOIN DSC_DW_MSTR.DBO.T_OB_HDR OB ON OB.OB_HDR_KEY = ORDERS.OB_HDR_KEY AND "+ 
			 " OB.DSC_LC_KEY = ORDERS.DSC_LC_KEY 	INNER JOIN DSC_DW_MSTR.DBO.M_DSC_LC LC " +
			 " ON LC.DSC_LC_KEY = ORDERS.DSC_LC_KEY 	INNER JOIN DSC_DW_MSTR.DBO.M_DSC_CUST CUST " +
			 " ON CUST.DSC_CUST_KEY = ORDERS.DSC_CUST_KEY 	INNER JOIN DSC_DW_MSTR.DBO.M_DT DATES ON " +
			 " DATES.DT_KEY = ORDERS.ACTUAL_SHIP_DT_KEY WHERE DATES.DT_CAL_DT BETWEEN "+range +
			 " GROUP BY LC.DSC_LC_CD,DATES.DT_CAL_MONTH_YEAR_CD,OB.PRIMARY_BLDG_ID "+
             " UNION ALL "+
             " SELECT RINB.LOCNBR AS LC_CODE,RINB.BLDNBR AS PRIMARY_BUILDING_ID,RINB.LOCNBR   + RINB.BLDNBR  as LCBLD, "+
             " DATENAME(YY, CONVERT(DATETIME,CAST(CONVERT(VARCHAR(8),RINB.INARDT) AS DATE),112)) + ' ' + RIGHT('0'+ " +
             " CAST(DATEPART(MM, CONVERT(DATETIME,CAST(CONVERT(VARCHAR(8),RINB.INARDT) AS DATE),112)) AS " +
             " VARCHAR(3)),2) AS MONTH_YEAR,0 AS TOTAL_CASES_SHIPPED,SUM(RINB.INCKCS) AS TOTAL_CASES_RECEIVED "+
             " FROM [DSC_DW_ARCHIVE].[DBO].[SRC_RINB_ARCHIVE] RINB "+
             " INNER JOIN [DSC_DW_MSTR].[DBO].[M_DSC_LC] LC ON LC.DSC_LC_CD = RINB.LOCNBR "+
             " INNER JOIN [DSC_DW_MSTR].[DBO].[M_DSC_CUST] CUST ON CUST.DSC_CUST_NBR = RINB.STRNBR "+
             " WHERE INARDT > 20130000 "+
             " AND CASE WHEN (INARDT = 0 OR INARDT = '20270006') THEN ('1900-01-01') " +
             " ELSE CAST(CAST(INARDT AS VARCHAR(10)) AS DATE) END BETWEEN "+range + 
             " GROUP BY RINB.LOCNBR,RINB.BLDNBR,DATENAME(YY, CONVERT(DATETIME,CAST(CONVERT(VARCHAR(8),RINB.INARDT) " +
             " AS DATE),112)) + ' ' + RIGHT('0'+ CAST(DATEPART(MM, CONVERT(DATETIME,CAST(CONVERT(VARCHAR(8),RINB.INARDT)"+
             " AS DATE),112)) AS VARCHAR(3)),2)) a group by LC_CODE, PRIMARY_BUILDING_ID, LCBLD,MONTH_YEAR "+
             " order by LC_CODe, PRIMARY_BUILDING_ID,MONTH_YEAR";
			 
 
 
	         
	          System.out.println("SQL is:"+SQL);    
		      //dwurl or mtrcurl
               theurl=geturl("mtrcurl");
	          Statement stmt = conn.createStatement();
 
			        ResultSet rs = stmt.executeQuery(SQL);
			        ResultSetMetaData rsmd = rs.getMetaData();
	 
			         BigDecimal thruput =new BigDecimal(0);
			         BigDecimal blkthruput =new BigDecimal(0);				         
			         BigDecimal thismonth =new BigDecimal(0);			         
			         BigDecimal volume =new BigDecimal(0);	
			         BigDecimal avgvolume =new BigDecimal(0);				         
			         BigDecimal cnt =new BigDecimal(0);		
			         
			         String[] strMonths = new String [300];
	 			      //  if ( Arrays.asList(dtarray).contains("yourValue")) {dtarray[1]="Y";
			         ArrayList<String> dateList = new ArrayList<String>();

					int numColumns = rsmd.getColumnCount(); 
					MathContext mc = new MathContext(4);
                    String mthyear="";
                    int avgcnt=0;
                    rcount=0;
                    int x=0;
                    String fndarray="N";
                    boolean contains = false;
                    String ipdata="";
                    
                    while (rs.next())
                    {
                    	bldid=rs.getString("LCBLD").trim();	
                    	 // first time you are here lookup xref from Metric via API call.
                    	 if (rcount == 0)
   					  		{   
                    		    ipdata= rs.getString("MONTH_YEAR").replace(' ','-').trim();
                    		    if (!ipdata.equals(null)) {dateList.add(ipdata);    x++;}
                    		 	docall();
                    		 	wrklcbld=bldid;
                    		 	wrkdscmtrclcbldid = dscmtrclcbldid;
                    		 //	System.out.println("First record. Saved bld:"+wrklcbld +" source bld:"+bldid +" lcbddid:"+dscmtrclcbldid +"Foundid is:"+foundid);
   	    		            } 
                    	 rcount++;
                    	   //iterate the String array
                    	  //   System.out.print(" About to search list. X is now:"+x);
                               ipdata=rs.getString("MONTH_YEAR").replace(' ','-').trim(); 
                               
                               //Load the list into a hashSet
                               Set<String> set = new HashSet<String>(dateList);
                               if (!set.contains(ipdata))
                               {      
                         	          dateList.add(ipdata);
                        	           x++;
                        	        }
                        	     
                        //	   System.out.println(". Completed Searching for:"+ipdata +" Variable X is now:"+x);

                    	 // for every record if the LCBLD changes do the restcall
                    	 if (wrklcbld != bldid)
                    	 {
                    		 docall();
                 		 	 
                    	 }
                 		 	 // if xref ID changed then dump the data in memory to a Json and rest wrk data areas
                 		  if ((!wrkdscmtrclcbldid.equals(dscmtrclcbldid)  && x > 0))
                 		  {
                 			// System.out.print("**** Break in Xref. Old one was:"+wrkdscmtrclcbldid +" New one is:"+dscmtrclcbldid);
                 			   avgcnt=x;
                 			    if (avgcnt == 0) avgcnt=1;
                 		 	    cnt = new BigDecimal(avgcnt);
                 		 	    
                 		 	//	 System.out.println(" Divide thismonth:"+thismonth +" thruput value:"+thruput +" divide count is:"+cnt);
                 		 		avgvolume=thruput.divide(cnt,2, RoundingMode.HALF_UP);
                 		 	    volume=(thismonth.subtract((thruput.divide(cnt,2, RoundingMode.HALF_UP))));
                 		 	    volume=volume.divide(avgvolume,2, RoundingMode.HALF_UP);
                 		 	//    System.out.println(" Volume is:"+volume);
       						    obj1 = new JSONObject();
       						    obj1.put("Primary_Building_ID", wrklcbld);
       						    obj1.put("dsc_mtrc_lc_bldg_id", wrkdscmtrclcbldid);
       						    obj1.put("Volume", thruput);
       						    obj1.put("Count", avgcnt);
    						    obj1.put("thismonth", thismonth);
    						    obj1.put("processYYMM", thismy);
       						    obj1.put("PeriodValue", volume);
                                jsona.put(obj1); 
                 		 		wrkdscmtrclcbldid = dscmtrclcbldid;
                 		 		thruput.equals(blkthruput);
                 		 		avgcnt=0;
                 		 		x=1;
                 		 		dateList.clear();
                 		 		ipdata=rs.getString("MONTH_YEAR").replace(' ','-').trim();
                 		 		dateList.add(ipdata);
                 		   }
                 		 wrklcbld=bldid;
                 		 try
                 		 {
                 		  if (foundid.equals("Y"))
                 		  {
                 			 fndarray="";
        					 avgcnt++;
    						 mthyear=rs.getString("MONTH_YEAR").replace(' ','-').trim();
     						 
    						// if (!Arrays.asList(dtarray).contains(mthyear)) {dtarray.add(mthyear.toString());x++;}   						 
    					 	// System.out.println("Compare input month:"+mthyear +" with requested date:"+thismy);
    						 if (mthyear.equals(thismy)) thismonth=rs.getBigDecimal("THROUGHPUT");
    							 
    						 long tput=(rs.getLong("THROUGHPUT"));						  
    						 thruput=thruput.add(rs.getBigDecimal("THROUGHPUT"));
                 		  }
                 		 }
                 		  catch (Exception e)
                 		  {
                 			 System.out.println("error is:"+e.getMessage());
                 		  }
                            
                    	 
                    } // end of while loop
                    
         
			              rs.close();
			             stmt.close();
			             if (conn != null) { conn.close();}
			            
			             if (x > 0)
			             {
                 			// System.out.print("**** Last Break in Xref. Old one was:"+wrkdscmtrclcbldid +" New one is:"+dscmtrclcbldid);
                 			  avgcnt=x;
               			    if (avgcnt == 0) avgcnt=1;
               		 	    cnt = new BigDecimal(avgcnt);
               		 	    
               		 	//	 System.out.println(" Divide thismonth:"+thismonth +" thruput value:"+thruput +" divide count is:"+cnt);
               		 		avgvolume=thruput.divide(cnt,2, RoundingMode.HALF_UP);
               		 	    volume=(thismonth.subtract((thruput.divide(cnt,2, RoundingMode.HALF_UP))));
               		 	    volume=volume.divide(avgvolume,2, RoundingMode.HALF_UP);
               		 	//    System.out.println(" Volume is:"+volume);
    						    obj1 = new JSONObject();
    						    obj1.put("Primary_Building_ID", wrklcbld);
    						    obj1.put("dsc_mtrc_lc_bldg_id", wrkdscmtrclcbldid);
    						    obj1.put("Volume", thruput);
    						    obj1.put("Count", avgcnt);
    						    obj1.put("thismonth", thismonth);
    						    obj1.put("processYYMM", thismy);
    						    obj1.put("PeriodValue", volume);
                             jsona.put(obj1); 
              		 		wrkdscmtrclcbldid = dscmtrclcbldid;
              		 		thruput.equals(blkthruput);
              		 		avgcnt=0;
			             }
                   
			           //  jsona.put(obj1);
			             
				     jobj.put("DSCWMSVolumes",jsona);      

				  }
				   catch (SQLException e) {
					// TODO Auto-generated catch block
					// e.printStackTrace();
	                String msg="DataWarehouse DB Query Failed.";
	                sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
	   	            rb=Response.ok(sb.toString()).build();
	   	            return rb;
				   }
              System.out.println("Json object being sent is:"+jobj.toString());
	         rb=Response.ok(jobj.toString()).build();
	         if (conn != null) 
	         {
	      	   try{
	      		   conn.close();
	      		  } catch(SQLException e)
	      	      {//e.printStackTrace();
		                String msg="DataWarehouse DB Query Failed.";
		                sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
		   	            rb=Response.ok(sb.toString()).build();
		   	            return rb;
	      			  }
	      	      }
	          
             return rb;
	}
	
	public void docall()
	{
		 
		JSONObject jo = new JSONObject();
		 String  param= " {'building':'" + bldid +"'}";	  
		  jo=callrest(theurl,"wmsbuilding",param);			
		 // System.out.println("Rest Call result is:"+jo.toString());
	       if(jo.has("dsc_mtrc_lc_bldg_id")) 
      	 {
      	   try {
			dscmtrclcbldid=jo.get("dsc_mtrc_lc_bldg_id").toString();
			
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}    
      	   foundid="Y";
      	 }
           else
           {    			   
	           foundid="N";
           }      
		
	}
	
	private String geturl(String theurl)
	{
		 String msg="";
			try {
			    Context ctx = new InitialContext();
			    ctx = (Context) ctx.lookup("java:comp/env");
			    msg = (String) ctx.lookup(theurl);
			}
			catch (NamingException e) {           
		           msg= "FAILED";	       	 
			}	
			
		 return msg;
	 
	}
	private JSONObject callrest (String theurl ,String resource, String query)
	 {
    
          
           
           String   msg = null;
			JSONObject api = new JSONObject();
			JSONObject obj1 = new JSONObject();
			JSONArray tput = new JSONArray();
		     URL url = null;
		    URLConnection urlc = null;
	        PrintStream ps = null;
	        BufferedReader br = null;
	        StringBuilder  responseStrBuildera = new StringBuilder();
			String data=null;
		    String l = null;
			String tmperiodid=null;
			String dscmtrclcbldid=null;
			String mtrcperiodid=null;
			String insstmt=null;
			String mtrcnayn=null;
	  	 
		    	 // Calling rest service
	 
		    		    url = null;
		    			try {
		    				url = new URL(theurl + resource);
		    				//System.out.print("Before Rest call:"+url);		  
		    			} catch (MalformedURLException e1) {
		    				// TODO Auto-generated catch block
		    				//e1.printStackTrace();
		    			}
	 
		    		     
		    		     // result will be:  {"building":"BP2"}
		    		     //make connection
		    		      urlc = null;
		    				try {
		    					urlc = url.openConnection();
		    				} catch (IOException e1) {
		    					// TODO Auto-generated catch block
		    					//e1.printStackTrace();
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
		    					//e1.printStackTrace();
		    				}
		    		        ps.print(query);
		    		        ps.close();

		    		        //get result
		    		          br = null;
		    				try {
		    					br = new BufferedReader(new InputStreamReader(urlc.getInputStream()));
		    				} catch (IOException e1) {
		    					// TODO Auto-generated catch block
		    					//e1.printStackTrace();
		    				}
		    				  data=null;
		    		          l = null;
		    		         responseStrBuildera = new StringBuilder();



		    			        try {
		    						while ((l=br.readLine())!=null) {
		    							data=data+l;
		    							responseStrBuildera.append(l);
		    						 //   System.out.println(l);
		    						}
		    					} catch (IOException e1) {
		    						// TODO Auto-generated catch block
		    						//e1.printStackTrace();
		    					}
		    		        try {
		    					br.close();
		    				} catch (IOException e1) {
		    					// TODO Auto-generated catch block
		    					//e1.printStackTrace();
		    				}		
		    		       try
		    		       {
		    		        if (data != null)     api = new JSONObject(responseStrBuildera.toString());
		    	 
		 
		    		       }
		    		       catch (Exception e1) {
		    					// TODO Auto-generated catch block
		    					//e1.printStackTrace();
		    				}	
		//    System.out.println("Rest call return for building is:"+api.toString());		  
		return api;
		 
		 
	 }
	
 
}
