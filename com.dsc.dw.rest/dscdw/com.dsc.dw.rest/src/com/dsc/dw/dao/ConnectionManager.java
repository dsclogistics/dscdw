package com.dsc.dw.dao;


import javax.naming.*;
import javax.sql.*;

public class ConnectionManager {
	
	private static DataSource dwDS = null;
	private static Context  context = null;

	
	public static DataSource mtrcConn() throws Exception
	{
		if (dwDS != null){
			return dwDS;
		}
		try{
			
			if (context == null){
				context = new InitialContext();
			}
			dwDS = (DataSource) context.lookup("java:/comp/env/dwDS");
		}
		catch( Exception e) {
			e.printStackTrace();
		}
		return dwDS;
	}

}
