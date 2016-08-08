package com.dsc.dw.internal;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.dsc.dw.dao.ConnectionManager;

public class LoadStatus {
	
	@SuppressWarnings("null")
	public Response LoadStatus(JSONObject inputJsonObj) throws JSONException {
		
		 Response rb = null;
		StringBuffer sb = new StringBuffer();
		StringBuffer sbn = new StringBuffer();
       JSONArray json = new JSONArray();
       JSONObject obj1 = new JSONObject();
       String status="N";
		String [] pkarray =null;
		String [] pkstatus=new String [12];
		
       
       // Get
    	 
       if (inputJsonObj.has("packagename") && inputJsonObj.has("calyear") && inputJsonObj.has("calmonth"))
      {
      	String pkgname= inputJsonObj.get("packagename").toString();
        String calyear= inputJsonObj.get("calyear").toString();
        String calmonth=inputJsonObj.get("calmonth").toString();
        String strtdtm ="01";
        int monthnum=0;
        String pname="";
        if (pkgname.equals("volume")) pname="'F_OB_ORDER_TIMELINE','SRC_RINB_Upsert'";
        if (pkgname.equals("netfte")) pname="'F_LM_FTE_PERFORMANCE'";     
        if (pkgname.equals("trainee%")) pname="'F_LM_FTE_PERFORMANCE'"; 
        String dateparm=calyear+"-"+calmonth +"-01";
       	 
			 Connection conn = null;
				try {
					conn= ConnectionManager.mtrcConn().getConnection();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
	                String msg="DataWarehouse DB Connection Failed.";
	                sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
	   	          rb=Response.ok(sb.toString()).build();
	   	          return rb;
				}
 
		 try {

			 String SQL=" SELECT [audp_pkg_name] ,[audp_start_dt] ,[audp_end_dt],[audp_success_ind] "+
						" FROM [SSISDB].[custom].[audit_pkg_run] "+
					    " where [audp_pkg_name] in ("+pname +")"+ 
						" and cast(audp_end_dt as date) = DATEADD(mm,1,DATEADD(mm, DATEDIFF(mm,0,'" +
					    dateparm +"'),0)) "+
					    " and audp_success_ind=1";
 
			  System.out.println("Sql for LoadStatus is:"+SQL);
	          Statement stmt = conn.createStatement();

			      // do starts here
			        ResultSet rs = stmt.executeQuery(SQL);
			        ResultSetMetaData rsmd = rs.getMetaData();
					int numColumns = rsmd.getColumnCount(); 				
					pkarray =pname.replace("'","").split(",");
				//	System.out.println("Pkg count array is:"+pkarray.length);
					for (int y=0; y < pkarray.length;y++) {pkstatus[y]="N";}
 
					while (rs.next()) {
					JSONObject obj = new JSONObject();
					for (int y=0; y < pkarray.length;y++)
					{
						if (pkarray[y].equals(rs.getString(1)))   pkstatus[y]="Y"; 
					}
 
                   
					for (int i=1; i<numColumns+1; i++) {
				        String column_name = rsmd.getColumnName(i);

				          obj.put(column_name, rs.getString(i));
				       
				        
					} // for numcolumns
					 json.put(obj);
					} // while loop
	 
			              rs.close();
			             stmt.close();
			             if (conn != null) { conn.close();}    
				     obj1.put("loadstatus",(Object)json);      

				  }
				   catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
	                String msg="DataWarehouse DB Query Failed.";
	                sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
	   	            rb=Response.ok(sb.toString()).build();
	   	            if (conn != null) 
	   	            {
	   	            	try{
	   	            		conn.close();
	   	            	} catch(SQLException e1)
	   	            	{e1.printStackTrace(); 
	   	            		 msg="DataWarehouse DB Connection dropped.";
	   	            		sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
	   	            		rb=Response.ok(sb.toString()).build();
	   	            	}
	   	            } 
	   	            return rb;
				   }
             
		   // verify if all the pkgnames you are looking are there. 
		    String msg="";
			for (int y=0; y < pkarray.length;y++)
			{
				if (pkstatus[y].equals("Y"))
				{
					status="Y";
				}
				else
				{
					status="N";
					msg=msg +pkarray[y] +"  ";
				}
			}
		     if (status.equals("Y")) 
		    	 {
		    	  rb=Response.ok(obj1.toString()).build();
		    	 }
		     else
		     {
	      	      msg=msg +" packages not complete. ";
                  sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
   	               rb=Response.ok(sb.toString()).build();		    	 
		     }
	         if (conn != null) 
	         {
	      	   try{
	      		   conn.close();
	      		  } catch(SQLException e)
	      	      {e.printStackTrace(); 
	      	        msg="DataWarehouse DB Connection dropped.";
                  sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
   	               rb=Response.ok(sb.toString()).build();
	      	      }
	         } 
      }
       else
       {
    	   String msg="Json elements: packagename , calyear  , calmonth required for this API ";
           sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"}");
	            rb=Response.ok(sb.toString()).build();
       }
            return rb;
	}

}
