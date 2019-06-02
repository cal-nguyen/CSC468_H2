/* Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
    private String table1;
    private String table2;
    private String mview;

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
        
        
        stat.execute("CREATE TABLE TABLE1(VALUE INT)");                    //base table 1
        stat.execute("CREATE TABLE TABLE2(VALUE INT)");                    //base table 2
        
        stat.execute("INSERT INTO TABLE1 VALUES(2)");                      
        stat.execute("INSERT INTO TABLE2 VALUES(4)");
        
        stat.execute("CREATE MATERIALIZED VIEW TEST(VALUE INT) AS SELECT * FROM TABLE2");    //all 4 commands assigned in parser
        
        
        
        
        
        
        stat.execute("INSERT INTO TABLE1 VALUES(1)");                     //trigger will fire on this line
        
        
        
        ResultSet rs;                                                    
        rs = stat.executeQuery("SELECT VALUE FROM TEST");                    
        rs.next();
        
        System.out.println("The first tuple of Test has value " + rs.getInt(1));   //prints tuple that was added to TEST upon creation
        
        
        rs.next();
        
        System.out.println("The second tuple of Test has value " + rs.getInt(1)); //prints tuple that was added by the trigger
        
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
    	
    	String s = "INSERT INTO  " + mview + " VALUES(?)";
    	PreparedStatement prep = conn.prepareStatement(s);
        prep.setInt(1, (int) row[0]);
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
            String test, String data2, String data3) throws SQLException {
    	/* Materialized view and base table strings are given to the trigger */
        TRIGGERS.get(getPrefix(conn) + trigger).table1 = data2;
        TRIGGERS.get(getPrefix(conn) + trigger).table2 = data3;
        TRIGGERS.get(getPrefix(conn) + trigger).mview = test;
        
        
    }

    private static String getPrefix(Connection conn) throws SQLException {
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery(
                "call ifnull(database_path() || '_', '') || database() || '_'");
        rs.next();
        return rs.getString(1);
    }

}
