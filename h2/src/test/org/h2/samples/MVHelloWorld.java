/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.h2.tools.DeleteDbFiles;

/**
 * A very simple class that shows how to load the driver, create a database,
 * create a table, and insert some data.
 */
public class MVHelloWorld {

    /**
     * Called when ran from command line.
     *
     * @param args ignored
     */
    public static void main(String... args) throws Exception {
        // delete the database named 'test' in the user home directory
        DeleteDbFiles.execute("~", "test", true);

        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection("jdbc:h2:~/test");
        Statement stat = conn.createStatement();

        // this line would initialize the database
        // from the SQL script file 'init.sql'
        // stat.execute("runscript from 'init.sql'");
        
        ResultSet rs;
        
        stat.execute("CREATE TABLE test(id int primary key, name varchar(255))");
        stat.execute("INSERT INTO test VALUES(1, 'Hello')");
        stat.execute("INSERT INTO test VALUES(2, 'World')");
        stat.execute("INSERT INTO test VALUES(3, '!!!')");

        stat.execute("CREATE MATERIALIZED VIEW mvname AS SELECT id, name FROM test");
        

        rs = stat.executeQuery("select * from mvname");
        System.out.println("\n\n\nOriginal Values from underlying table");
        System.out.println("id\t\tname");
        System.out.println("-----------------------");
        while (rs.next()) {
            System.out.print(rs.getString("id"));
            System.out.println("\t\t" + rs.getString("name"));
        }
        
        
        stat.execute("INSERT INTO test VALUES(4, 'CSC')");
        stat.execute("INSERT INTO test VALUES(5, '468')");
        stat.execute("INSERT INTO test VALUES(6, 'DBMS')");
        rs = stat.executeQuery("select * from mvname");

        System.out.println("\nValues after underlying table INSERT");
        System.out.println("id\t\tname");
        System.out.println("-----------------------");
        while (rs.next()) {
            System.out.print(rs.getString("id"));
            System.out.println("\t\t" + rs.getString("name"));
        }
        

        
        stat.close();
        conn.close();
    }

}
