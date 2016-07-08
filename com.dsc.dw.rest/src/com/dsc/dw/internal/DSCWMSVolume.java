package com.dsc.dw.internal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.sql.ResultSetMetaData;

import javax.ws.rs.core.Response;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONArray;
 


import com.dsc.dw.dao.*; 


public class DSCWMSVolume  {
	 
	
	public Response DSCWMSVolume(JSONObject inputJsonObj) throws JSONException {
		
		 Response rb = null;
		StringBuffer sb = new StringBuffer();
		StringBuffer sbn = new StringBuffer();
        JSONArray json = new JSONArray();
        JSONObject obj1 = new JSONObject();
        
        // Get tp 
     	 
     	// if (s1.get("tpt_name").toString().equals("COLLECTED")) 
       	 
         String calyear= inputJsonObj.get("calyear").toString();
         String calmonth=inputJsonObj.get("calmonth").toString();
         String strtdtm ="01";
         int monthnum=0;
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
					e.printStackTrace();
	                String msg="DataWarehouse DB Connection Failed.";
	                sb.append("{\"result\":\"FAILED\",\"resultCode\":200,\"message\":\""+msg+"\"");
	   	          rb=Response.ok(obj1.toString()).build();
	   	          return rb;
				}
  
		 try {
 
			 String SQL="SELECT a.LC_CODE,a.PRIMARY_BUILDING_ID,MONTH_YEAR,a.LC_CODE +a.PRIMARY_BUILDING_ID as LCBLD, "+
" substring(Month_year,1,4) + substring(Month_year,6,2) as YYMM, case when(b.throughput = 0) then 0 else  "+
" ((sum(a.TOTAL_CASES_SHIPPED) + sum(a.TOTAL_CASES_RECEIVED)) / 2 - b.THROUGHPUT)/b.THROUGHPUT end as THROUGHPUT_PCT_CHG "+
" from ( "+
" 	SELECT 	LC.DSC_LC_CD AS LC_CODE,OB.PRIMARY_BLDG_ID AS PRIMARY_BUILDING_ID,DATES.DT_CAL_MONTH_YEAR_CD AS MONTH_YEAR, "+
" 	SUM(ORDERS.CASES_SHIPPED_QTY) AS TOTAL_CASES_SHIPPED, 0 AS TOTAL_CASES_RECEIVED	FROM DSC_DW_MSTR.DBO.F_OB_ORDER_TIMELINE ORDERS "+
" 	LEFT OUTER JOIN DSC_DW_MSTR.DBO.T_OB_HDR OB ON OB.OB_HDR_KEY = ORDERS.OB_HDR_KEY AND OB.DSC_LC_KEY = ORDERS.DSC_LC_KEY "+
" 	INNER JOIN DSC_DW_MSTR.DBO.M_DSC_LC LC ON LC.DSC_LC_KEY = ORDERS.DSC_LC_KEY "+
" 	INNER JOIN DSC_DW_MSTR.DBO.M_DT DATES ON DATES.DT_KEY = ORDERS.ACTUAL_SHIP_DT_KEY "+
" 	GROUP BY LC.DSC_LC_CD, 	DATES.DT_CAL_MONTH_YEAR_CD,	OB.PRIMARY_BLDG_ID "+
" 	UNION ALL "+
" 	SELECT 	RINB.LOCNBR AS LC_CODE,	RINB.BLDNBR AS PRIMARY_BUILDING_ID, "+
" 	DATENAME(YY, CONVERT(DATETIME,CAST(CONVERT(VARCHAR(8),RINB.INARDT) AS DATE),112)) + ' ' +  "+
" 	RIGHT('0'+ CAST(DATEPART(MM, CONVERT(DATETIME,CAST(CONVERT(VARCHAR(8),RINB.INARDT) AS DATE),112)) AS VARCHAR(3)),2) AS MONTH_YEAR, "+
" 	0 AS TOTAL_CASES_SHIPPED,	SUM(RINB.INCKCS) AS TOTAL_CASES_RECEIVED FROM [DSC_DW_ARCHIVE].[DBO].[SRC_RINB_ARCHIVE] RINB "+
" 	INNER JOIN [DSC_DW_MSTR].[DBO].[M_DSC_LC] LC ON LC.DSC_LC_CD = RINB.LOCNBR "+
" 	WHERE INARDT > 20130000 GROUP BY RINB.LOCNBR,RINB.STRNBR,RINB.BLDNBR, "+
" 	DATENAME(YY, CONVERT(DATETIME,CAST(CONVERT(VARCHAR(8),RINB.INARDT) AS DATE),112)) + ' ' + "+
" 	RIGHT('0'+ CAST(DATEPART(MM, CONVERT(DATETIME,CAST(CONVERT(VARCHAR(8),RINB.INARDT) AS DATE),112)) AS VARCHAR(3)),2)) a "+
" JOIN ( "+
" 	SELECT "+
" 	LC_CODE,PRIMARY_BUILDING_ID,(sum(TOTAL_CASES_SHIPPED) + sum(TOTAL_CASES_RECEIVED)) / count(LC_CODE) as THROUGHPUT "+
" 	from ( "+
" 		SELECT LC.DSC_LC_CD AS LC_CODE,	OB.PRIMARY_BLDG_ID AS PRIMARY_BUILDING_ID,DATES.DT_CAL_MONTH_YEAR_CD AS MONTH_YEAR, "+
" 		SUM(ORDERS.CASES_SHIPPED_QTY) AS TOTAL_CASES_SHIPPED, 0 AS TOTAL_CASES_RECEIVED "+
" 		FROM DSC_DW_MSTR.DBO.F_OB_ORDER_TIMELINE ORDERS "+
" 		LEFT OUTER JOIN DSC_DW_MSTR.DBO.T_OB_HDR OB ON OB.OB_HDR_KEY = ORDERS.OB_HDR_KEY AND OB.DSC_LC_KEY = ORDERS.DSC_LC_KEY "+
" 		INNER JOIN DSC_DW_MSTR.DBO.M_DSC_LC LC ON LC.DSC_LC_KEY = ORDERS.DSC_LC_KEY "+
" 		INNER JOIN DSC_DW_MSTR.DBO.M_DSC_CUST CUST ON CUST.DSC_CUST_KEY = ORDERS.DSC_CUST_KEY "+
" 		INNER JOIN DSC_DW_MSTR.DBO.M_DT DATES ON DATES.DT_KEY = ORDERS.ACTUAL_SHIP_DT_KEY "+
" 		WHERE DATES.DT_CAL_DT BETWEEN CONVERT(VARCHAR(25),DATEADD(dd,-(DAY(DATEADD(mm,-12,CAST(GETDATE() AS DATE)))-1), "+
" 		DATEADD(mm,-12,CAST(GETDATE() AS DATE))),101) AND CONVERT(VARCHAR(25),DATEADD(dd,-(DAY(CAST(GETDATE() AS DATE))),CAST(GETDATE() AS DATE)),101) "+
" 		GROUP BY LC.DSC_LC_CD, DATES.DT_CAL_MONTH_YEAR_CD, OB.PRIMARY_BLDG_ID "+
" 		UNION ALL "+
" 		SELECT  "+
" 		RINB.LOCNBR AS LC_CODE,	RINB.BLDNBR AS PRIMARY_BUILDING_ID, "+
" 		DATENAME(YY, CONVERT(DATETIME,CAST(CONVERT(VARCHAR(8),RINB.INARDT) AS DATE),112)) + ' ' + "+
" 		RIGHT('0'+ CAST(DATEPART(MM, CONVERT(DATETIME,CAST(CONVERT(VARCHAR(8),RINB.INARDT) AS DATE),112)) AS VARCHAR(3)),2) AS MONTH_YEAR, "+
" 		0 AS TOTAL_CASES_SHIPPED,SUM(RINB.INCKCS) AS TOTAL_CASES_RECEIVED "+
" 		FROM [DSC_DW_ARCHIVE].[DBO].[SRC_RINB_ARCHIVE] RINB "+
" 		INNER JOIN [DSC_DW_MSTR].[DBO].[M_DSC_LC] LC ON LC.DSC_LC_CD = RINB.LOCNBR "+
" 		INNER JOIN [DSC_DW_MSTR].[DBO].[M_DSC_CUST] CUST ON CUST.DSC_CUST_NBR = RINB.STRNBR "+
" 		WHERE INARDT > 20130000 "+
" 		AND CASE WHEN (INARDT = 0 OR INARDT = '20270006') THEN ('1900-01-01') ELSE CAST(CAST(INARDT AS VARCHAR(10)) AS DATE) END BETWEEN CONVERT(VARCHAR(25),"+
" 		DATEADD(dd,-(DAY(DATEADD(mm,-12,CAST(GETDATE() AS DATE)))-1),DATEADD(mm,-12,CAST(GETDATE() AS DATE))),101) AND "+ 
" 		CONVERT(VARCHAR(25),DATEADD(dd,-(DAY(CAST(GETDATE() AS DATE))),CAST(GETDATE() AS DATE)),101)   "+
" 		GROUP BY RINB.LOCNBR,	RINB.BLDNBR, "+
" 		DATENAME(YY, CONVERT(DATETIME,CAST(CONVERT(VARCHAR(8),RINB.INARDT) AS DATE),112)) + ' ' + "+
" 		RIGHT('0'+ CAST(DATEPART(MM, CONVERT(DATETIME,CAST(CONVERT(VARCHAR(8),RINB.INARDT) AS DATE),112)) AS VARCHAR(3)),2)) a "+
" 	where PRIMARY_BUILDING_ID IS NOT NULL and a.PRIMARY_BUILDING_ID <> ''	group by LC_CODE,PRIMARY_BUILDING_ID) b "+
"    on a.LC_CODE = b.LC_CODE and a.PRIMARY_BUILDING_ID = b.PRIMARY_BUILDING_ID "+
"     WHERE a.PRIMARY_BUILDING_ID IS NOT NULL and a.PRIMARY_BUILDING_ID <> ''  and left(MONTH_YEAR, 4) ="+calyear +"  and right(MONTH_YEAR, 2) ="+strtdtm +
" 	group by a.LC_CODE,a.PRIMARY_BUILDING_ID,a.MONTH_YEAR,b.THROUGHPUT ORDER BY LC_CODE ";
 
	         
	      System.out.println("SQL is:"+SQL);    
		    
	        
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
				     obj1.put("DSCWMSVolumes",(Object)json);      

				  }
				   catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
	                String msg="DataWarehouse DB Query Failed.";
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
