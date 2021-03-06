package com.dsc.dw.internal;
 

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSetMetaData;

import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.dsc.dw.dao.*;

import org.codehaus.jettison.json.JSONArray;

public class BuildingLc  {
	 
	
	public Response BuildingLc(JSONObject inputJsonObj) throws JSONException {
		
		 Response rb = null;
		StringBuffer sb = new StringBuffer();
		StringBuffer sbn = new StringBuffer();
        JSONArray json = new JSONArray();
        JSONObject obj1 = new JSONObject();
			 Connection conn = null;
 				try {
					conn= ConnectionManager.mtrcConn().getConnection();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
	                String msg="Metric DB Connection Failed.";
	                sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"");
	   	          rb=Response.ok(obj1.toString()).build();
	   	          return rb;
				}
  
		 try {
 
    		  String SQL = " select  a.[dsc_mtrc_lc_bldg_id] " +
    		  			   " ,a.[dsc_lc_id] "+
    		  			   " ,a.[dsc_mtrc_lc_bldg_name] "+
    		  			   ",a.[dsc_mtrc_lc_bldg_code] "+
    		  			   ",a.[dsc_mtrc_lc_bldg_eff_start_dt] "+
    		  			   ",a.[dsc_mtrc_lc_bldg_eff_end_dt] "+
    		  			   ",b.dsc_lc_name "+
    		  			   ",b.dsc_lc_code "+
    		  			   ",b.dsc_lc_timezone "+
    		  			   "  FROM  [dbo].[DSC_MTRC_LC_BLDG] a "+
    		  			   " ,[dbo].[DSC_LC] b "+
    		  			   " where a.dsc_lc_id=b.dsc_lc_id";
	         
	          
		  //   System.out.println("First SQL:" + SQL);
	        
	          Statement stmt = conn.createStatement();
	        //     System.out.println("statement connect done" );
			      // do starts here
			        ResultSet rs = stmt.executeQuery(SQL);
			        ResultSetMetaData rsmd = rs.getMetaData();
			//        System.out.println("result set created" );
 
					int numColumns = rsmd.getColumnCount(); 
					while (rs.next()) {

					JSONObject obj = new JSONObject();
 
					for (int i=1; i<numColumns+1; i++) {
				        String column_name = rsmd.getColumnName(i);

				          obj.put(column_name, rs.getString(i));
				       
				       
				        
					} // for numcolumns
					 json.put(obj);
					} // while loop
	 
			              rs.close();
			             stmt.close();
			             if (conn != null) { conn.close();} 
				     obj1.put("resource",(Object)json);      
				  }
				   catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
	                String msg="Metric DB Query Failed.";
	                sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"");
	   	            rb=Response.ok(obj1.toString()).build();
	   	            return rb;
				   }
 
	         rb=Response.ok(obj1.toString()).build();
	         if (conn != null) 
	         {
	      	   try{
	      		   conn.close();
	      		  } catch(SQLException e)
	      	      {e.printStackTrace(); }
	         } 
             return rb;
	}
}

 

