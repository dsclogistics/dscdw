package com.dsc.dw.internal;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public   class DateMagic {
 
	public static String  MonthDiffa(int year, String month, int offset)
	{
		String date2 = year +"-"+month;
	    SimpleDateFormat sdf = new SimpleDateFormat("yyyy MMM dd");
	    SimpleDateFormat sdfm = new SimpleDateFormat("MMMM-yyyy");
	    SimpleDateFormat sdfmm = new SimpleDateFormat("M");

		Calendar calendar = new GregorianCalendar(year,0,28);

		//System.out.println("Date : " + sdf.format(calendar.getTime()));
		// set date using month and year format
		try{
		calendar.setTime(sdfm.parse(date2));}
		catch(Exception ee){ System.out.println("");}
	
		//add one month
		calendar.add(Calendar.MONTH, 3);
		System.out.println("Date plus month 3 : " + sdf.format(calendar.getTime()));
	
		//subtract 10 days
		calendar.add(Calendar.DAY_OF_MONTH, -10);
      System.out.println("Date  day -10: " + sdfm.format(calendar.getTime()));
      
       // Get Month Number 
       System.out.println("MonthNumber: " + sdfmm.format(calendar.getTime()));
	return date2;	
		
	}
	public static String  MonthDiff(int year, String month, int offset)
	{
		String date2 = month +"-"+Integer.toString(year);
	    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MMM-dd");
	    SimpleDateFormat sdfm = new SimpleDateFormat("MMMM-yyyy");
		Calendar calendar = new GregorianCalendar(year,0,28);
		// set date using month and year format
		try{
		calendar.setTime(sdfm.parse(date2));}
		catch(Exception ee){ return "";}
		//add one month
		calendar.add(Calendar.MONTH, offset);
		System.out.println("Date plus month 3 : " + sdf.format(calendar.getTime()));
		return sdf.format(calendar.getTime());
	
		 
	} 
	public String  MonthRange(int year, String month, int offset)
	{
		 // Month has to be in Alpha ex: June
		String date2 = month +"-"+Integer.toString(year);
	    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MMM-dd");
		  SimpleDateFormat sdfn = new SimpleDateFormat("yyyy-MM-dd");
	    SimpleDateFormat mmmyyyy = new SimpleDateFormat("MMMM-yyyy");
	    SimpleDateFormat sdfm = new SimpleDateFormat("MMMM-yyyy");
		Calendar calendar = new GregorianCalendar(year,0,28);
		Calendar calendar2 = new GregorianCalendar(year,0,28);
		// set date using month and year format
		try{
		calendar.setTime(sdfm.parse(date2));
		calendar2.setTime(sdfm.parse(date2));
		}
		catch(Exception ee){ }
		//add one month
		System.out.println("Date Starting : " + sdfn.format(calendar.getTime()));
		calendar2.add(Calendar.MONTH,+1);
		calendar.add(Calendar.MONTH, offset);

		calendar2.add(Calendar.DAY_OF_MONTH,-1);
		//System.out.println("Date Offset : " + sdfn.format(calendar.getTime()));
		//System.out.println("Last Day of Month : " + sdfn.format(calendar2.getTime()));
	   // System.out.println("Long Month Year:" + mmmyyyy.format(calendar.getTime())); 
	    String range="'"+sdfn.format(calendar.getTime()) +"' and '" +sdfn.format(calendar2.getTime()) +"'";
		return range;
	
		 
	} 

}

