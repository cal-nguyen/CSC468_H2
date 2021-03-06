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
        deleteDb("mview");
        testEmptyColumn();
        testSingleColumn();
        testMultipleColumn();
        testAllColumn();
        testWhere();
        deleteDb("mview");
    }
    
    private void testEmptyColumn() throws SQLException {
        deleteDb("mview");
        Connection conn = getConnection("mview");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE Test(a INT, b INT)");
        stat.execute("CREATE MATERIALIZED VIEW test_view AS SELECT a, b FROM Test");
        stat.execute("SELECT * FROM test_view");
        conn.close();
    }
    
    private void testSingleColumn() throws SQLException {
        deleteDb("mview");
        Connection conn = getConnection("mview");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE Test(a INT, b INT)");
        stat.execute("INSERT INTO Test VALUES (1, 2)");
        stat.execute("CREATE MATERIALIZED VIEW test_view AS SELECT a FROM Test");
        
        ResultSet rs = stat.executeQuery("SELECT * FROM test_view");
        rs.next();
        assertEquals(1, rs.getInt(1));
        
        conn.close();
    }
    
    private void testMultipleColumn() throws SQLException {
        deleteDb("mview");
        Connection conn = getConnection("mview");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE Test(a INT, b INT, c INT, d INT)");
        stat.execute("INSERT INTO Test VALUES (1, 2, 3, 4)");
        stat.execute("CREATE MATERIALIZED VIEW test_view AS SELECT a, b, c, d FROM Test");
        
        ResultSet rs = stat.executeQuery("SELECT * FROM test_view");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        assertEquals(3, rs.getInt(3));
        assertEquals(4, rs.getInt(4));
        
        conn.close();
    }
    
    private void testAllColumn() throws SQLException {
        deleteDb("mview");
        Connection conn = getConnection("mview");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE Test(a INT, b INT)");
        stat.execute("INSERT INTO Test VALUES (1, 2)");
        stat.execute("CREATE MATERIALIZED VIEW test_view AS SELECT a, b FROM Test");
        
        ResultSet rs = stat.executeQuery("SELECT * FROM test_view");
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        
        conn.close();
    }
    
    private void testWhere() throws SQLException {
    	deleteDb("mview");
        Connection conn = getConnection("mview");
        Statement stat = conn.createStatement();
        stat.execute("CREATE TABLE Test(a INT, b INT)");
        stat.execute("INSERT INTO Test VALUES (1, 2), (2, 4)");
        stat.execute("CREATE MATERIALIZED VIEW test_view AS SELECT a, b FROM Test WHERE a = 2");
        
        ResultSet rs = stat.executeQuery("SELECT * FROM test_view");
        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals(4, rs.getInt(2));
        
        conn.close();
    }
    
    /*
     * The functionality for the cases below were not implemented, so they are not executed.
     */

    private void testInnerJoin() throws SQLException {
        deleteDb("mview");
        Connection conn = getConnection("mview");
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
        deleteDb("mview");
        Connection conn = getConnection("mview");
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
}
