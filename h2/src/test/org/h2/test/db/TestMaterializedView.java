/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.api.ErrorCode;
import org.h2.engine.Session;
import org.h2.jdbc.JdbcConnection;
import org.h2.test.TestBase;
import org.h2.test.TestDb;

/**
 * Test for views.
 */
public class TestMaterializedView extends TestDb {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws SQLException {
        deleteDb("view");
        testEmptyColumn();
        testSingleColumn();
        testInnerJoin();
        testView();
        deleteDb("view");
    }
    
    private void testEmptyColumn() throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE Test(a INT, b INT)");
        stat.execute("CREATE MATERIALIZED VIEW test_view AS SELECT a, b FROM Test");
        stat.execute("SELECT * FROM test_view WHERE a between 1 and 2 and b = 2");
        conn.close();
    }
    
    private void testSingleColumn() throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE Test(a INT, b INT)");
        stat.execute("INSERT INTO Test VALUES (1, 2)");
        stat.execute("CREATE MATERIALIZED VIEW test_view AS SELECT a FROM Test");
        
        ResultSet rs = stat.executeQuery("SELECT * FROM test_view");
        rs.next();
        assertEquals(1, rs.getInt(1));
        
        conn.close();
    }
    
    private void testInnerJoin() throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE First(id INT PRIMARY KEY, name VARCHAR(25))");
        stat.execute("INSERT INTO First VALUES ('Bob'), ('Carly')");
        stat.execute("CREATE TABLE Second(name VARCHAR(25), age INT)");
        stat.execute("INSERT INTO Second VALUES ('John', 25), ('Bob', 14)");
        stat.execute("CREATE MATERIALIZED VIEW test_view "
        		+ "AS SELECT age FROM Second "
        		+ "INNER JOIN First ON Second.name=First.name");
        
        ResultSet rs = stat.executeQuery("SELECT * FROM test_view");
        rs.next();
        assertEquals(14, rs.getInt(1));
        
        conn.close();
    }
    
    private void testView() throws SQLException {
        deleteDb("view");
        Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE Test(a INT, b INT)");
        stat.execute("INSERT INTO Test VALUES (1, 2)");
        stat.execute("CREATE VIEW v AS SELECT a FROM Test");
        stat.execute("CREATE MATERIALIZED VIEW mv AS SELECT * FROM v");
        
        ResultSet rs = stat.executeQuery("SELECT * FROM test_view");
        rs.next();
        assertEquals(1, rs.getInt(1));
        
        conn.close();
    }
    
    // Need to complete this test case
    private void testDropView() throws SQLException {
    	deleteDb("view");
    	Connection conn = getConnection("view");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE Test(a INT, b INT)");
        stat.execute("INSERT INTO Test VALUES (1, 2)");
        stat.execute("CREATE MATERIALIZED VIEW test_view AS SELECT a FROM Test");
        
        stat.execute("DROP MATERIALIZED VIEW test_view");
        
        conn.close();
    }
}
