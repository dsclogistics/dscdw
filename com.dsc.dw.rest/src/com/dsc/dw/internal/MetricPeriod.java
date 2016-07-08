
package com.dsc.dw.internal;


import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.sql.ResultSetMetaData;

import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.dsc.dw.dao.*;

import org.codehaus.jettison.json.JSONArray;


public class MetricPeriod  {
	 
	
	public Response MetricPeriod(JSONObject inputJsonObj) throws JSONException {
		
		 Response rb = null;
		StringBuffer sb = new StringBuffer();
		StringBuffer sbn = new StringBuffer();
        JSONArray json = new JSONArray();
        JSONObject obj1 = new JSONObject();
        JSONObject objg = new JSONObject();
         DecimalFormat df2 = new DecimalFormat(".##");
         df2.setRoundingMode(RoundingMode.UP);


        
        String tptname= inputJsonObj.get("tptname").toString();
        String prodname=inputJsonObj.get("productname").toString();
        String calmonth=inputJsonObj.get("calmonth").toString();
        String calyear=inputJsonObj.get("calyear").toString();
        String mtrcid=inputJsonObj.get("mtrcid").toString();
        String master="N";
        String detail="N";
        String pctyn="N";
 
        	 
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
			 
			 // first get header level data
 
			 
			 String SQL1=" select mmper.mtrc_period_id,  mm.mtrc_name, mm.mtrc_id, "+
				         " mmper.mtrc_period_name, pt.tpt_name, tp.tm_per_start_dtm, "+
				         " tp.tm_per_end_dtm, mp.prod_name,mdt.data_type_token, mdt.data_type_num_yn, " +
				         " COALESCE(mmper.mtrc_period_min_val,mm.mtrc_min_val) as mtrc_min_val," +
				         " COALESCE(mmper.mtrc_period_max_val,mm.mtrc_max_val) as mtrc_max_val, "+
				         " COALESCE(mmper.mtrc_period_max_dec_places,mm.mtrc_max_dec_places) as mtrc_max_dec_places,"+
				         " COALESCE(mmper.mtrc_period_max_str_size,mm.mtrc_max_str_size)as mtrc_max_str_size, "+
				         " mmper.mtrc_period_na_allow_yn,mmper.mtrc_period_can_import_yn, " +
				         " mtrc_period_is_auto_yn ,rmps.rz_mps_status,tp.tm_period_id" +
				         " from mtrc_product mp "+
				         " join MTRC_METRIC_PRODUCTS mmp on mp.prod_id = mmp.prod_id "+
				         " join MTRC_METRIC_PERIOD  mmper on mmp.mtrc_prod_id = mmper.Mtrc_period_id "+
				         " join MTRC_METRIC mm 	on mmper.mtrc_id = mm.mtrc_id "+			         
 						 " join MTRC_DATA_TYPE mdt  on mm.data_type_id = mdt.data_type_id "+		         
				         " join MTRC_TIME_PERIOD_TYPE pt on mmper.tpt_id = pt.tpt_id "+
				         " join MTRC_TM_PERIODS tp on pt.tpt_id = tp.tpt_id "+
				         " join rz_mtrc_period_status rmps on rmps.mtrc_period_id =mmper.mtrc_period_id "+
					     " and rmps.tm_period_id = tp.tm_period_id  and rmps.rz_mps_status <> 'Inactive' "+
				         " where mp.prod_name ='Red Zone' and pt.tpt_name = '"+tptname +"' "+
				         " and mm.mtrc_id ="+mtrcid +" and Year(tp.tm_per_start_dtm)="+calyear +
				         " and Year(tp.tm_per_end_dtm)=" +calyear +" and  "+
				         " Datename(month,tp.tm_per_start_dtm) ='"+ calmonth +"'"+
				         " and Datename(month,tp.tm_per_start_dtm) ='"+calmonth +"' ";
			 
			 
			 
			 
 
    		  String SQL = " select bldg.dsc_mtrc_lc_bldg_name, bldg.dsc_mtrc_lc_bldg_id, bmp_is_editable_yn, " +
    		               " bmp_is_manual_yn ,  bmp_na_allow_yn, " +
    		  		        " value.mtrc_period_val_id,value.UserId," +
    				        " VALUE.mtrc_period_val_value from "+
    				       " (select DSC_MTRC_LC_BLDG.dsc_mtrc_lc_bldg_name, "+
    				       " DSC_MTRC_LC_BLDG.dsc_mtrc_lc_bldg_id, "+
    				       " MTRC_BLDG_MTRC_PERIOD.bmp_is_editable_yn, "+
    				       " MTRC_BLDG_MTRC_PERIOD.bmp_is_manual_yn, "+
    				       "   MTRC_BLDG_MTRC_PERIOD.bmp_na_allow_yn, "+
    				       " MTRC_METRIC_PERIOD.mtrc_period_id "+
    				       " from MTRC_BLDG_MTRC_PERIOD,MTRC_METRIC_PERIOD, DSC_MTRC_LC_BLDG "+
    				       " where MTRC_METRIC_PERIOD.mtrc_period_id = MTRC_BLDG_MTRC_PERIOD.mtrc_period_id "+
    				       " and (dsc_mtrc_lc_bldg.dsc_mtrc_lc_bldg_eff_start_dt  >= getDate() and "+
    				       " dsc_mtrc_lc_bldg.dsc_mtrc_lc_bldg_eff_end_dt <=getDate()) "+
    				       " and MTRC_BLDG_MTRC_PERIOD.dsc_mtrc_lc_bldg_id = DSC_MTRC_LC_BLDG.dsc_mtrc_lc_bldg_id "+
    				       " and  MTRC_METRIC_PERIOD.mtrc_id ="+mtrcid +") bldg  "+
    				       " left outer join (select MTRC_METRIC_PERIOD_VALUE.dsc_mtrc_lc_bldg_id, "+
    				       " MTRC_METRIC_PERIOD_VALUE.mtrc_period_val_id,MTRC_METRIC_PERIOD_VALUE.mtrc_period_val_value ,"+
    				       " coalesce(MTRC_METRIC_PERIOD_VALUE.mtrc_period_val_upd_by_user_id, "+
    				       " MTRC_METRIC_PERIOD_VALUE.mtrc_period_val_added_by_usr_id) as UserID,"+
    				       " MTRC_METRIC_PERIOD_VALUE.mtrc_period_id " +
    				       " from MTRC_METRIC_PERIOD_VALUE join MTRC_TM_PERIODS on" +	  				       
    				       " MTRC_METRIC_PERIOD_VALUE.tm_period_id =MTRC_TM_PERIODS.tm_period_id "+
    				       " where Year(MTRC_TM_PERIODS.tm_per_start_dtm)="+calyear +
    				       " and Year(MTRC_TM_PERIODS.tm_per_end_dtm)="+calyear +
    				       " and Datename(month,MTRC_TM_PERIODS.tm_per_start_dtm) ='"+calmonth +"' "+
    				       " and Datename(month,MTRC_TM_PERIODS.tm_per_start_dtm) ='" +calmonth +"' ) value " +
    				       " on bldg.mtrc_period_id =value.mtrc_period_id and "+
    				       "  bldg.dsc_mtrc_lc_bldg_id = value.dsc_mtrc_lc_bldg_id";
  		  
	         
	          
		     System.out.println("Sql:"+SQL);
 
	          Statement stmt = conn.createStatement();
	 
	          ResultSet rs = stmt.executeQuery(SQL1);
	          ResultSetMetaData rsmd = rs.getMetaData();
			        int tmpid=0;
			        int mpid=0;
			        
                   int  numColumns = rsmd.getColumnCount(); 
                     JSONObject jo = new JSONObject(); 
					while (rs.next()) {
						
						if (detail.equals("N")) detail="Y";
					for (int i=1; i<numColumns+1; i++) {
				        String column_name = rsmd.getColumnName(i);
				        String colvalue=rs.getString(i);
				        if (column_name.equals("mtrc_period_id")) mpid=rs.getInt(i);
				        if (column_name.equals("tm_period_id")) tmpid=rs.getInt(i);
				        if (column_name.equals("data_type_token"))
				        	{
				        	  if (colvalue.equals("pct")) pctyn="Y"; 
				        	}
				        try
				        {
				        	if (pctyn.equals("Y"))
				        	{
				        	  
				        		Double cvalue;
				        		if (column_name.equals("mtrc_min_val"))	{
				        		  cvalue=Double.parseDouble(colvalue) * 100;
							      //colvalue=Float.toString(cvalue);
							      colvalue=df2.format(cvalue);}
				        		if (column_name.equals("mtrc_max_val"))	{
				        		  cvalue=Double.parseDouble(colvalue) * 100;
							      colvalue=df2.format(cvalue);}  					        	
				        	}
				        }
				        catch(Exception e)
				        {
				         System.out.println("Invalid Percent value:"+colvalue);	
				        }
				         
				        
				        if (colvalue  == null ) {colvalue="";}
				          jo.put(column_name, colvalue);   
					 } // for numcolumns
 
					} // while loop

					  rs.close();
					// now get status for prior or next period status
			      int tmpidp=tmpid-1;
			      int tmpidn=tmpid+1;
			      String minprd="";
			      String maxprd="";
				SQL1="select tm_period_id, rz_mps_status from rz_mtrc_period_status where tm_period_id " +
				" in ("+ tmpidp +","+tmpidn +") and mtrc_period_id="+mpid +" and rz_mps_status <> 'Inactive' "+
						" order by tm_period_id ";
	           // System.out.println("Sql is:"+SQL);
		          rs = stmt.executeQuery(SQL1);
		          rsmd = rs.getMetaData();
		          numColumns = rsmd.getColumnCount(); 
					while (rs.next()) 
					{
				       if (rs.getInt(1) == tmpidp) minprd=rs.getString(2);
				       if (rs.getInt(1) == tmpidn) maxprd=rs.getString(2);
 			
					}
					
					 jo.put("previousperiod", minprd); 
					 jo.put("nextperiod", maxprd);

					 
		              rs.close();						 
// now do sql for detail tables:
					 
				          rs = stmt.executeQuery(SQL);
				          rsmd = rs.getMetaData();
			 
	 
						  numColumns = rsmd.getColumnCount(); 
						//  System.out.println("NumColumns for locationdetails:"+numColumns);
						while (rs.next()) {
	                    if (master.equals("N")) master="Y";
						JSONObject obj = new JSONObject();
	 
						for (int i=1; i<numColumns+1; i++) {
					        
					        String column_name = rsmd.getColumnName(i);
					        String colvalue=rs.getString(i);

					        try
					        {
					        if ((pctyn.equals("Y"))  && (!colvalue.equals(null) || !colvalue.trim().isEmpty()))
					        {
					        	Double cvalue;;
								if (column_name.equals("mtrc_period_val_value"))
								{
									// System.out.println("Metric Value is:"+colvalue +" bld:"+rs.getString(1));
							 
									  cvalue=Double.parseDouble(colvalue)*100;
									 // df2.format(input));
					        		 // cvalue=Float.parseFloat(colvalue)*100;
					        		 // System.out.println("Cvalue after multiply is:"+cvalue);
								     // colvalue=Float.toString(cvalue);
									  colvalue=df2.format(cvalue);
								     // System.out.println("Cvalue float to string is:"+colvalue);
								     }	
								      	        	
					        	}
					        }
					        catch(Exception e)
					        {
					        //	System.out.println("Exception :"+e);
					        }
					        
					        if (colvalue  == null ) {colvalue="";}				        
					          obj.put(column_name, colvalue);
	     
						} // for numcolumns
						 json.put(obj);
						} // while loop
		 
				 
					 
					 
					if ((master.equals("Y")) && (detail.equals("Y")))
					{
						obj1.put("metricdetail",jo);
						obj1.put("locationdetails",json);
					}
			              rs.close();		     
			             stmt.close();
			             if (conn != null) { conn.close();} 
      
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

 

