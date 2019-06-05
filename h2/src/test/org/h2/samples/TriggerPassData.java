/* Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;

import org.h2.api.Trigger;

/**
 * This sample application shows how to pass data to a trigger. Trigger data can
 * be persisted by storing it in the database.
 */
public class TriggerPassData implements Trigger {

    private static final ConcurrentHashMap<String, TriggerPassData> TRIGGERS = new ConcurrentHashMap<>();
    
    private String mview;
    private String table1;
    private String table2;
    
    private String attribute1;
    private String attribute2;
    
    private String where1;
    private String where2;

    /**
     * This method is called when executing this sample application from the
     * command line.
     *
     * @param args the command line parameters
     */
    public static void main(String... args) throws Exception {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection(
                "jdbc:h2:mem:test", "sa", "");
        Statement stat = conn.createStatement();
        
        
        stat.execute("CREATE TABLE TABLE1(VALUE INT, NAME VARCHAR)");                    //base table 1
        stat.execute("CREATE TABLE TABLE2(VALUE INT, LOCATION VARCHAR)");                    //base table 2
        
        stat.execute("INSERT INTO TABLE1 VALUES(2, 'Thomas Bramble')");                      
        stat.execute("INSERT INTO TABLE2 VALUES(4, 'San Diego')");
        stat.execute("INSERT INTO TABLE2  VALUES(2, 'Paso Robles')");
        
        stat.execute("CREATE MATERIALIZED VIEW TEST(NAME VARCHAR, LOCATION VARCHAR) AS SELECT NAME, LOCATION FROM "
        		+ "TABLE1, TABLE2 WHERE TABLE1.VALUE = TABLE2.VALUE");    //all 4 commands assigned in parser
        
        
        
        
        
        
        stat.execute("INSERT INTO TABLE1 VALUES(2, 'Wes Janson')");                     //trigger will fire on this line
        
        
        
        ResultSet rs;                                                    
        rs = stat.executeQuery("SELECT * FROM TEST");                    
        rs.next();
        
        System.out.println("The first tuple of Test has name " + rs.getString(1) + " and location " + rs.getString(2));   //prints tuple that was added to TEST upon creation
        
        
        rs.next();
        
        System.out.println("The second tuple of Test has name " + rs.getString(1) + " and location " + rs.getString(2)); //prints tuple that was added by the trigger
        
        stat.close();
        conn.close();
    }

    @Override
    public void init(Connection conn, String schemaName,
            String triggerName, String tableName, boolean before,
            int type) throws SQLException {
        TRIGGERS.put(getPrefix(conn) + triggerName, this);
    }

    @Override
    public void fire(Connection conn, Object[] old, Object[] row) throws SQLException {
    	
    	String s;
    	Statement stat;
    	PreparedStatement prep;
    	ResultSet rs;
    	
    	/* Select all of the columns from first table for metadata (we need value of join attribute for a query) */
    	s = "SELECT * FROM " + table1;                 
    	stat = conn.createStatement();
        
        rs = stat.executeQuery(s);
        rs.next();
        
        /*cName will iterate through columns, cValue will store join attribute value, jName will store name of join attribute */
        String cName, cValue = null, jTemp = null, jName = null;  
        ResultSetMetaData rsmd = rs.getMetaData();
        
        if (where1 != null) {
	        for (int j = 1; j < rsmd.getColumnCount()+1; j++) {            //loop through columns of first table
	        	cName = rsmd.getColumnName(j);
	        	jTemp = cName;
	        	if (table2 != null)
	        		cName = table1 + "." + cName;                             //join attribute will be written as table1.attribute
	        	if (cName.contentEquals(where1)) {                        //if cName contains name of join attribute
	        		jName = jTemp;
	        		if (rsmd.getColumnTypeName(j).contentEquals("INT"))
	        			cValue = (String) row[j-1];                       //get int value of join attribute for the tuple inserted
	        		else
	        			cValue = "'" + row[j-1] + "'";                    //get string value of join attribute for the tuple inserted
	        		System.out.println("SUCCESS!");
	        	}
	        }
        }
        
        
        
        int k = 0;           //keeps track of whether 1 or 2 attributes is selected
        
        /*Set up sql statement that will select tuples containing all values of tuple inserted into base table */
        if (attribute2 == null) {
        	k++;
    		s = "SELECT " + attribute1; 
        }
    								
    	else {
    		s = "SELECT " + attribute1 + ",  " + attribute2;
    	}
                         //if two tables are join with where condition
		if (table2 != null) {
    		s = s + " FROM " + table1 + ", " + table2 + " WHERE " + where2 + " = " + cValue; //value of join attribute is used
		}
		else if (where1 != null) {
			s = s + " FROM " + table1 + " WHERE " + where1 + " = " + where2;
		}
		else {
			s = s + " FROM " + table1 + " WHERE ";
		}
		
		for (int i = 1; i <= row.length; i++) {
			
			cName = rsmd.getColumnName(i);
			
			if (where1 != null) {
				if (cName.contentEquals(jName)) {          //if cName is join attribute, set it equal to table1.attribute
					cName = where1;
				}
				if (rsmd.getColumnTypeName(i).contentEquals("INT"))     //add where conditions to string
					s = s + " AND " + cName + " = " + row[i-1];
				else
					s = s + " AND " + cName + " = " + "'" + row[i-1] + "'";
			}
			else {
				if (i != 1) {
					if (rsmd.getColumnTypeName(i).contentEquals("INT"))     //add where conditions to string
						s = s + " AND " + cName + " = " + row[i-1];
					else
						s = s + " AND " + cName + " = " + "'" + row[i-1] + "'";
				}
				else {
					if (rsmd.getColumnTypeName(i).contentEquals("INT"))     //add where conditions to string
						s = s + cName + " = " + row[i-1];
					else
						s = s + cName + " = " + "'" + row[i-1] + "'";
				}
			}
				
		}
		
    		
    	
        
        
        
        rs = stat.executeQuery(s);
        rs.next();
        rsmd = rs.getMetaData();
        
        /*Construct insertion statement*/
        s = "INSERT INTO " + mview + " VALUES(";
        k++;
        while (k < 2) {
        	
        	if (rsmd.getColumnTypeName(k).contentEquals("INT"))
        		s = s + rs.getInt(k) + ", ";
        	else
        		s = s + "'" + rs.getString(k) + "'" + ", ";
        	
        	System.out.println(mview);
        	k++;
        }
        
        if (rsmd.getColumnTypeName(k).contentEquals("INT"))
    		s = s + rs.getInt(k) + ")";
    	else
    		s = s + "'" + rs.getString(k) + "'" + ")";
        
        prep = conn.prepareStatement(s);
        prep.execute();
        
        
    }

    @Override
    public void close() {
        // ignore
    }

    @Override
    public void remove() {
        // ignore
    }

    /**
     * Call this method to change a specific trigger.
     *
     * @param conn the connection
     * @param trigger the trigger name
     * @param data the data
     */
    
    public static void setTriggerData(Connection conn, String trigger,
            String mview, String table1, String table2, String attribute1,
            String attribute2, String where1, String where2) throws SQLException {
    	/* Materialized view and base table strings are given to the trigger */
        TRIGGERS.get(getPrefix(conn) + trigger).table1 = table1;
        TRIGGERS.get(getPrefix(conn) + trigger).table2 = table2;
        TRIGGERS.get(getPrefix(conn) + trigger).mview = mview;
        
        TRIGGERS.get(getPrefix(conn) + trigger).attribute1 = attribute1;
        TRIGGERS.get(getPrefix(conn) + trigger).attribute2 = attribute2;
        TRIGGERS.get(getPrefix(conn) + trigger).where1 = where1;
        TRIGGERS.get(getPrefix(conn) + trigger).where2 = where2;
        
        System.out.println(mview);
        System.out.println(table1);
        System.out.println(table2);
        
        System.out.println(attribute1);
        System.out.println(attribute2);
        System.out.println(where1);
        System.out.println(where2);
    }

    private static String getPrefix(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(
                "call ifnull(database_path() || '_', '') || database() || '_'");
        rs.next();
        return rs.getString(1);
    }

}
