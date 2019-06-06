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
    private String triggerType;
    
    private String attribute1;
    private String attribute2;
    private String attribute3;
    private String attribute4;
    private String attribute5;
    
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
        
        
        stat.execute("CREATE TABLE TABLE1(IDENTITY INT, NAME VARCHAR)");                    //base table 1
        stat.execute("CREATE TABLE TABLE2(ID INT, LOCATION VARCHAR)");                    //base table 2
        
        stat.execute("INSERT INTO TABLE1 VALUES(2, 'Thomas Bramble')");                      
        stat.execute("INSERT INTO TABLE2 VALUES(4, 'San Diego')");
        stat.execute("INSERT INTO TABLE2  VALUES(2, 'Paso Robles')");
        stat.execute("INSERT INTO TABLE2 VALUES(2, 'San Luis Obispo')");
        
        
        stat.execute("CREATE MATERIALIZED VIEW TEST(ID INT, LOCATION VARCHAR) AS SELECT ID, LOCATION FROM "
        		+ "TABLE2 WHERE ID = '2'");    //all 4 commands assigned in parser
        
        //stat.execute("UPDATE TABLE2 SET LOCATION = 'Arroyo Grande' WHERE ID = 4");
        
        
        
        
        stat.execute("INSERT INTO TABLE1 VALUES(2, 'Wes Janson')");                     //trigger will fire on this line
        
        stat.execute("INSERT INTO TABLE2 VALUES(2, 'Los Angeles')");
        
        ResultSet rs;                                                    
        rs = stat.executeQuery("SELECT * FROM TEST");           
        rs.next();
        System.out.println("The first tuple of Test has name " + rs.getString(1) + " and location " + rs.getString(2));
        rs.next();
        System.out.println("The second tuple of Test has name " + rs.getString(1) + " and location " + rs.getString(2));
        rs.next();
        System.out.println("The third tuple of Test has name " + rs.getString(1) + " and location " + rs.getString(2));
        
        
        
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
    	if (triggerType.contentEquals("Insert"))
    		insert(conn, old, row);
    	
    	
        
        
    }
    
    private void insert(Connection conn, Object[] old, Object[] row) throws SQLException {
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
        String cName;
        ResultSetMetaData rsmd = rs.getMetaData();
        
        
        
        
        
        int k = 0;           //keeps track of whether 1 or 2 attributes is selected
        
        /*Set up sql statement that will select tuples containing all values of tuple inserted into base table */
        
        if (attribute2 == null) {
        	k = 4;
    		s = "SELECT " + attribute1; 
        }
    								
    	else if (attribute3 == null) {
    		k = 3;
    		s = "SELECT " + attribute1 + ",  " + attribute2;
    	}
    	else if (attribute4 == null) {
    		k = 2;
    		s = "SELECT " + attribute1 + ",  " + attribute2 + ", " + attribute3;
    	}
    	else if (attribute5 == null) {
    		k = 1;
    		s = "SELECT " + attribute1 + ",  " + attribute2 + ", " + attribute3 + ", " + attribute4;
    	}
    	else {
    		
    		s = "SELECT " + attribute1 + ",  " + attribute2 + ", " + attribute3 + ", " + attribute4 + ", " + attribute5;
    	}
    		
                         //if two tables are join with where condition
		if (table2 != null) {
			if (where1 != null)
				s = s + " FROM " + table1 + ", " + table2 + " WHERE " + where1 + " = " + where2; //value of join attribute is used
			else
				s = s + " FROM " + table1 + ", " + table2 + " WHERE ";
		}
		else  {
			if (where1 != null)
				s = s + " FROM " + table1 + " WHERE " + where1 + " = " + where2;
			else {
				s = s + " FROM " + table1 + " WHERE ";
			}
		}
		
		
		for (int i = 1; i <= row.length; i++) {
			
			cName = rsmd.getColumnName(i);
			
			if (where1 != null) {
				
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
		 
        
        int q;
        rs = stat.executeQuery(s);
        while (rs.next()) {
        	rsmd = rs.getMetaData();
        	
	        /*Construct insertion statement*/
	        s = "INSERT INTO " + mview + " VALUES(";
	        q = k;
	        while (q < 5) {
	        	
	        	if (rsmd.getColumnTypeName(q).contentEquals("INT"))
	        		s = s + rs.getInt(q) + ", ";
	        	else
	        		s = s + "'" + rs.getString(q) + "'" + ", ";
	        	
	        	
	        	q++;
	        }
	        
	        if (rsmd.getColumnTypeName(q).contentEquals("INT"))
	    		s = s + rs.getInt(q) + ")";
	    	else
	    		s = s + "'" + rs.getString(q) + "'" + ")";
	        
	        prep = conn.prepareStatement(s);
	        prep.execute();
        }
    }

    private void update(Connection conn, Object[] old, Object[] row) throws SQLException {
    	String s;
    	Statement stat;
    	PreparedStatement prep;
    	ResultSet rs;
    	
    	/* Select all of the columns from first table for metadata (we need value of join attribute for a query) */
    	s = "SELECT * FROM " + table1;                 
    	stat = conn.createStatement();
        
        rs = stat.executeQuery(s);
        rs.next();
        
        s = "UPDATE " + mview + " SET ";
        Boolean commaBit = false;
        String cName;  
        ResultSetMetaData rsmd = rs.getMetaData();
        
        
        for (int j = 1; j < row.length+1; j++) {            //loop through columns of first table
        	if (!row[j-1].equals(old[j-1])) {
        	
        		cName = rsmd.getColumnName(j);
        		if (commaBit)
        			s = s + ", ";
        		if (cName.contentEquals(attribute1) || cName.contentEquals(attribute2)) {
        			if (rsmd.getColumnTypeName(j).contentEquals("INT"))
        				s = s + cName + " = " + row[j-1];
        			else
        				s = s + cName + " = " + "'" + row[j-1] + "'";
        			commaBit = true;
        		}
        			
        		
        	}
        }
        
        Boolean andBit = false;
        s = s + " WHERE ";
        for (int j = 1; j < row.length+1; j++) {
        	cName = rsmd.getColumnName(j);
 
    		if (andBit)
    			s = s + " AND ";
    		if (cName.contentEquals(attribute1) || cName.contentEquals(attribute2)) {
    			if (rsmd.getColumnTypeName(j).contentEquals("INT"))
    				s = s + cName + " = " + old[j-1];
    			else
    				s = s + cName + " = " + "'" + old[j-1] + "'";
    			commaBit = true;
    		}
        }
        
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
            String attribute2, String attribute3, String attribute4, String attribute5,
            String where1, String where2) throws SQLException {
    	/* Materialized view and base table strings are given to the trigger */
        TRIGGERS.get(getPrefix(conn) + trigger).table1 = table1;
        TRIGGERS.get(getPrefix(conn) + trigger).table2 = table2;
        TRIGGERS.get(getPrefix(conn) + trigger).mview = mview;
        
        TRIGGERS.get(getPrefix(conn) + trigger).attribute1 = attribute1;
        TRIGGERS.get(getPrefix(conn) + trigger).attribute2 = attribute2;
        TRIGGERS.get(getPrefix(conn) + trigger).attribute3 = attribute3;
        TRIGGERS.get(getPrefix(conn) + trigger).attribute4 = attribute4;
        TRIGGERS.get(getPrefix(conn) + trigger).attribute5 = attribute5;
        		
        
        
        TRIGGERS.get(getPrefix(conn) + trigger).where1 = where1;
        TRIGGERS.get(getPrefix(conn) + trigger).where2 = where2;
        TRIGGERS.get(getPrefix(conn) + trigger).triggerType = "Insert";
        System.out.println("mView: " + mview);
        System.out.println("table1: " + table1);
        System.out.println("table2: " + table2);
        
        System.out.println("attribute1: " + attribute1);
        System.out.println("attribute2: " + attribute2);
        System.out.println("where1: " + where1);
        System.out.println("where2: " + where2);
    }

    private static String getPrefix(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(
                "call ifnull(database_path() || '_', '') || database() || '_'");
        rs.next();
        return rs.getString(1);
    }

}
