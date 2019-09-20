
/* Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.*/
/*
   DESCRIPTION    
   The code sample shows how to use the DataSource API to establish a connection
   to the Database. You can specify properties with "setConnectionProperties".
   This is the recommended way to create connections to the Database.

   Note that an instance of oracle.jdbc.pool.OracleDataSource doesn't provide
   any connection pooling. It's just a connection factory. A connection pool,
   such as Universal Connection Pool (UCP), can be configured to use an
   instance of oracle.jdbc.pool.OracleDataSource to create connections and 
   then cache them.
    
    Step 1: Enter the Database details in this file. 
            DB_USER, DB_PASSWORD and DB_URL are required
    Step 2: Run the sample with "ant DataSourceSample"
  
   NOTES
    Use JDK 1.7 and above

   MODIFIED    (MM/DD/YY)
    nbsundar    02/17/15 - Creation 
 */
 
 //C:\ora11gr2\product\11.2.0\client_1\network\admin\tnsnames.ora
 //"c:\Program Files\Java\jdk1.8.0_144\bin\javac.exe" -classpath ojdbc8.jar Dump_JSON_1.java Group_Row.java Group_Key.java
 //"c:\Program Files\Java\jdk1.8.0_144\bin\java.exe" -cp .;ojdbc8.jar Dump_JSON_1

import java.io.IOException;
import java.io.InputStream;
import java.io.FileWriter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.Properties;
import java.util.HashMap;      // import the HashMap class
import java.util.ArrayList;

import oracle.jdbc.pool.OracleDataSource;
import oracle.jdbc.OracleConnection;
import java.sql.DatabaseMetaData;

public class Dump_JSON_1 {  
  // The recommended format of a connection URL is the long format with the
  // connection descriptor.
  
  final static String DB_URL= "jdbc:oracle:thin:@host:1521:servicename";
  
  // For ATP and ADW - use the TNS Alias name along with the TNS_ADMIN when using 18.3 JDBC driver
  // final static String DB_URL="jdbc:oracle:thin:@wallet_dbname?TNS_ADMIN=/Users/test/wallet_dbname";
  // In case of windows, use the following URL 
  // final static String DB_URL="jdbc:oracle:thin:@wallet_dbname?TNS_ADMIN=C:\\Users\\test\\wallet_dbname";
  final static String DB_USER = "user";
  final static String DB_PASSWORD = "pass";

 /*
  * The method gets a database connection using 
  * oracle.jdbc.pool.OracleDataSource. It also sets some connection 
  * level properties, such as,
  * OracleConnection.CONNECTION_PROPERTY_DEFAULT_ROW_PREFETCH,
  * OracleConnection.CONNECTION_PROPERTY_THIN_NET_CHECKSUM_TYPES, etc.,
  * There are many other connection related properties. Refer to 
  * the OracleConnection interface to find more. 
  */
  public static void main(String args[]) throws SQLException {
    Properties info = new Properties();     
    info.put(OracleConnection.CONNECTION_PROPERTY_USER_NAME, DB_USER);
    info.put(OracleConnection.CONNECTION_PROPERTY_PASSWORD, DB_PASSWORD);          
    info.put(OracleConnection.CONNECTION_PROPERTY_DEFAULT_ROW_PREFETCH, "100");    
  

    OracleDataSource ods = new OracleDataSource();
    ods.setURL(DB_URL);    
    ods.setConnectionProperties(info);

    // With AutoCloseable, the connection is closed automatically.
    try (OracleConnection connection = (OracleConnection) ods.getConnection()) {
      // Get the JDBC driver name and version 
      DatabaseMetaData dbmd = connection.getMetaData();       
      System.out.println("Driver Name: " + dbmd.getDriverName());
      System.out.println("Driver Version: " + dbmd.getDriverVersion());
      // Print some connection properties
      System.out.println("Default Row Prefetch Value is: " + connection.getDefaultRowPrefetch());
      System.out.println("Database Username is: " + connection.getUserName());
      
      connection.setDefaultRowPrefetch(300);
      System.out.println("Changing Default Row Prefetch Value to 300 !!!!!");
      System.out.println("Default Row Prefetch Value is: " + connection.getDefaultRowPrefetch());
      
      System.out.println();
      
      // Perform a database operation 
      GetQueryResults(connection);
    }
    
    System.out.println("main DONE!!!!!");
  }

  
  public static void DumpFileOutput(ArrayList<Output_JSON> jsonlist)
  {
	  String temp;
	  
	  for (int ii=0; ii < jsonlist.size(); ii++)
	  {
		  Output_JSON json = jsonlist.get(ii);
		  
		  temp = String.format("Dumping %s %s", json.OutputJSONFilename, json.BF_FORM_INSC_CD);
		  System.out.println(temp);
		  
		  try 
		  {
		  
			  FileWriter myfile = new FileWriter(json.OutputJSONFilename);
			  
			  myfile.write("{\n");
			  
			  temp = String.format("'agencyCode':\"%s\",\n", json.AgencyCode);
			  myfile.write(temp);
			  
			  temp = String.format("'agencyName':\"%s\",\n", json.AgencyName);
			  myfile.write(temp);
			  
			  temp = String.format("'isDivision':%s,\n", json.IsDivision);
			  myfile.write(temp);
			  
			  myfile.write("'content': [\n");
			  
			  int len = json.Content.size();
			  int lastindex = len - 1;
			  for (int cc=0; cc < len; cc++)
			  {
				  myfile.write(json.Content.get(cc));
				  
				  if (cc == lastindex)
					  myfile.write("\n");
				  else
					  myfile.write(",\n");
			  }
	
			  myfile.write("]\n");
			  myfile.write("}\n");
			  
			  myfile.close();
		  }
		  catch (IOException ee)
		  {
			  temp = String.format("ERROR CAUGHT !!!!! Dumping %s %s", json.OutputJSONFilename, json.BF_FORM_INSC_CD);
			  System.out.println(temp);
			  
			  ee.printStackTrace();
			  return;
		  } 
	  }
  }

 
  public static void Build_JSON(HashMap<Group_Key, ArrayList<Group_Row>>  my_map) 
  {
	  ArrayList<Output_JSON> jsonlist = new ArrayList<Output_JSON>();
	  String temp;
	  
	  for (Group_Key my_key : my_map.keySet())
	  {
		  ArrayList<Group_Row> my_rows = my_map.get(my_key);
		  
		  Output_JSON my_json = new Output_JSON(my_key.OutputJSONFilename, my_key.AgencyCode, my_key.AgencyName, my_key.IsDivision, my_key.BF_FORM_INSC_CD);
		  
		  /************************************************************************
		  		--      content:
				--                (1) ok group rows by BF_FORM_INSC_CD from table BF_FORM_INSC_DCTN (ex. 10951 or 10962)
				--                (2) subgroup by TAB_NUM from table BF_FORM_INSC_DCTN (ex. 1 or 2 or 3)
				--                   
				--                    foreach row in rows grouped by distinct TAB_NUM
				--                    {
				--                         bool put_first_tab = false;
				--                         if ((put_first_tab == false) && (DCTN1_LBL != "Program Description") && (DCTN1_LBL != (null)))
				--                         {
				--                                 print ["TAB", DCTN1_LBL]
				--                                 print [SEC_TITL, SEC_CNTN]
				--                                 put_first_tab = true;
				--                         }
				--                         else if ((put_first_tab == true) && (DCTN1_LBL != "Program Description") && (DCTN1_LBL != (null))) 
				--                                 print [SEC_TITL, SEC_CNTN]
				--                    }
		  *********************************************************************/
		  
		  Boolean put_first_tab = false;
		  String last_seen_TAB_NUM = "";
		  for (int rr=0; rr < my_rows.size(); rr++)
		  {
			  Group_Row row = my_rows.get(rr);
			  
			  //subgroup by TAB_NUM
			  if (last_seen_TAB_NUM.equals(row.TAB_NUM) == false)
			  {
				  last_seen_TAB_NUM = row.TAB_NUM;
				  put_first_tab = false;
			  }
			  
			  //do output
			  if ((put_first_tab == false) && (row.DCTN1_LBL.equals("Program Description") == false) && (row.DCTN1_LBL.equals("null") == false))
			  {
				  put_first_tab = true;
				  
				  temp = String.format("[\"TAB\", \"%s\"]", row.DCTN1_LBL);
				  my_json.Content.add(temp);
				  
				  temp = String.format("[\"%s\", \"%s\"]",  row.SEC_TITL, row.SEC_CNTN);
				  my_json.Content.add(temp);	
			  }
			  else if (put_first_tab && (row.DCTN1_LBL.equals("Program Description") == false) && (row.DCTN1_LBL.equals("null") == false))
			  {
				  temp = String.format("[\"%s\", \"%s\"]",  row.SEC_TITL, row.SEC_CNTN);
				  my_json.Content.add(temp);	
			  }
		  }
		  
		  jsonlist.add(my_json);
	  }//for
	  
	  DumpFileOutput(jsonlist);
  }//Build_JSON


  
  public static void GetQueryResults(Connection connection) throws SQLException
  {
	  
    // Statement and ResultSet are AutoCloseable and closed automatically. 
	
	//my_map is hashmap
	//where key is basically code == BF_FORM_INSC_CD (for example 10951 or 10962)
	//and value is arraylist of rows grouped by code
	//assume for now my_agencyCode, my_agencyName, my_isDivision is same for all rows grouped by code
	//because they want 1 json file per code
		
	int result_row_count = 0;
	int unknown_narrative_count = 1;
	String last_seen_code = "";
	ArrayList<Group_Row> my_rows = null;
	Group_Key my_key = null;
	HashMap<Group_Key, ArrayList<Group_Row>>  my_map = new HashMap<Group_Key, ArrayList<Group_Row>>();
    try (Statement statement = connection.createStatement()) {

      String bigquerystr = " select BF_FORM_INSC_BF_FORM_DEF_CD," +
                           "        BF_FORM_INSC_CD," +
	                       "        TAB_NUM," +
	                       "        tabs.DCTN1_LBL," +
	                       "        SEQ_NUM," +
	                       "        SEC_TITL," +
	                       "        SEC_CNTN," +
                           "        BF_FORM_DEF.ORGN_LEVL," +
                           "        bform.BF_ORGN_ID," +
	                       "        bform.nm," +
	                       "        bform.DSCR" +
                           " from            BF_FORM_INSC_DCTN" +
                           " left outer join BF_FORM_INSC bform   on (BF_FORM_INSC_CD = bform.CD)" +
                           " left outer join BF_FORM_DEF          on (BF_FORM_INSC_BF_FORM_DEF_CD = BF_FORM_DEF.CD)" +
                           " join (" +
                           "        select CD, DCTN1_LBL, 1 as tab" +
                           "        from BF_FORM_DEF" +
                           "        where cd in ('DEPT-NARRATIVE', 'DIV-NARRATIVE')" +
                           "        Union" +
                           "        select CD, DCTN2_LBL, 2  as tab" +
                           "        from BF_FORM_DEF" +
                           "        where cd in ('DEPT-NARRATIVE', 'DIV-NARRATIVE')" +
                           "        Union" +
                           "        select CD, DCTN3_LBL, 3  as tab" +
                           "        from BF_FORM_DEF" +
                           "        where cd in ('DEPT-NARRATIVE', 'DIV-NARRATIVE')" +
                           "      ) tabs on tabs.tab = tab_num" +
                           " where BF_FORM_INSC_BF_FORM_DEF_CD in ('DEPT-NARRATIVE', 'DIV-NARRATIVE')" +
                           " order by BF_FORM_INSC_BF_FORM_DEF_CD, BF_FORM_INSC_CD, TAB_NUM, SEQ_NUM";

		
      try (ResultSet resultSet = statement.executeQuery(bigquerystr)) {
		
        while (resultSet.next()) {
			
			//System.out.println(resultSet.getString(1) + " - " + resultSet.getString(2) + " ");

        	result_row_count++;
        	
			String BF_FORM_INSC_BF_FORM_DEF_CD = resultSet.getString(1);
			String BF_FORM_INSC_CD             = resultSet.getString(2);
			String TAB_NUM                     = resultSet.getString(3);
			String DCTN1_LBL                   = resultSet.getString(4);
			String SEQ_NUM                     = resultSet.getString(5);
			String SEC_TITL                    = resultSet.getString(6);
			String SEC_CNTN                    = resultSet.getString(7);
			String BF_ORGN_ID                  = resultSet.getString(9);
			String DSCR                        = resultSet.getString(11);
			
			/*****************************************************************************
			From Database sql query results to intermediate storage before file output
			******************************************************************************/
			
			BF_FORM_INSC_BF_FORM_DEF_CD = BF_FORM_INSC_BF_FORM_DEF_CD.toUpperCase().trim();
			BF_FORM_INSC_CD             = BF_FORM_INSC_CD.trim();
			TAB_NUM                     = TAB_NUM.trim();
			
			//sometimes in results DCTN1_LBL is null
			if (DCTN1_LBL != null)
				DCTN1_LBL               = DCTN1_LBL.trim();
			else
				DCTN1_LBL               = "null";
			
			SEQ_NUM                     = SEQ_NUM.trim();
			SEC_TITL                    = SEC_TITL.trim().toUpperCase(); //must do this i saw a "n" when everything else is like "N" or "BL1" or "BL2"
			
			//sometimes in results SEC_CNTN is null
			if (SEC_CNTN != null)
				SEC_CNTN                = SEC_CNTN.trim();
			else
				SEC_CNTN                = "null";
			
			//little algorithm for BF_ORGN_ID to find correct 3 character agency code:
			//if starts with "OP_" then take last 3 characters starting from index
			//otherwise take first 3 chars.
			String my_agencyCode        = BF_ORGN_ID.substring(0,6).trim().toUpperCase();
			if (my_agencyCode.startsWith("OP_"))
				my_agencyCode           = my_agencyCode.substring(3);
			else
				my_agencyCode           = my_agencyCode.substring(0,3);
			
			
			String my_agencyName        = DSCR.trim();
			
			String my_isDivision;
			String my_output_json_filename;
			if (BF_FORM_INSC_BF_FORM_DEF_CD.equals("DEPT-NARRATIVE"))
			{
				my_isDivision = "false";
				
				//from requirements .xlsx dept->OP_BFS.json as example
				my_output_json_filename = BF_ORGN_ID.substring(0,6).trim().toUpperCase() + ".json";
			}
			else if (BF_FORM_INSC_BF_FORM_DEF_CD.equals("DIV-NARRATIVE"))
			{
				my_isDivision = "true";
				
				//from requirements .xlsx div->BFS0300C.json as example
				my_output_json_filename = BF_ORGN_ID.substring(0,8).trim().toUpperCase() + ".json";
			}
			else
			{
				my_isDivision = "UNKNOWN-NARRATIVE";
				
				my_output_json_filename = String.format("UNKNOWN-NARRATIVE%d.json", unknown_narrative_count);
				unknown_narrative_count++;
			}
			
			//group by BF_FORM_INSC_CD (ex. 10951 or 10962)
			//assume for now my_agencyCode, my_agencyName, my_isDivision is same for all grouped rows by BF_FORM_INSC_CD
			//because they want 1 json file per grouped rows by BF_FORM_INSC_CD
			if (last_seen_code.equals(BF_FORM_INSC_CD) == false)
			{
				last_seen_code = BF_FORM_INSC_CD;
				if (my_rows != null && my_key != null)	
					my_map.put(my_key, my_rows);
				
				my_key  = new Group_Key(my_output_json_filename, my_agencyCode, my_agencyName, my_isDivision, BF_FORM_INSC_CD);
				my_rows = new ArrayList<Group_Row>();
				
				Group_Row  temp = new Group_Row(TAB_NUM, DCTN1_LBL, SEQ_NUM, SEC_TITL, SEC_CNTN);
				my_rows.add(temp);
			}
			else
			{
				Group_Row  temp = new Group_Row(TAB_NUM, DCTN1_LBL, SEQ_NUM, SEC_TITL, SEC_CNTN);
				my_rows.add(temp);
			}
		}//while rows
		
		//do not forget to add very last set of rows grouped by BF_FORM_INSC_CD
		if (my_rows != null && my_key != null)	
			my_map.put(my_key, my_rows);
		
      }//try
    }//try
    
    String debugstr = String.format("GetQueryResults result_row_count=%d", result_row_count);
    System.out.println(debugstr);

    Build_JSON(my_map);	
  }//GetQueryResults 

  
}//class Dump_JSON_1


