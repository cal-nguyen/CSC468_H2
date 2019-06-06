package org.h2.test.db;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.tools.DeleteDbFiles;
import org.junit.Test;



public class MVUnitTest {
	
	  @Test
	  public void MVTest2Columns() throws ClassNotFoundException, SQLException {
		  
	        // delete the database named 'test' in the user home directory
	        DeleteDbFiles.execute("~", "test", true);

	        Class.forName("org.h2.Driver");
	        Connection conn = DriverManager.getConnection("jdbc:h2:~/test");
	        Statement stat = conn.createStatement();		  
	        ResultSet rs;
	        
	        //
	        //Test with two columns
	        //
	        stat.execute("CREATE TABLE test(id int primary key, name varchar(255))");
	        stat.execute("INSERT INTO test VALUES(1, 'Hello')");
	        stat.execute("INSERT INTO test VALUES(2, 'World')");
	        stat.execute("INSERT INTO test VALUES(3, '!!!')");

	        stat.execute("CREATE MATERIALIZED VIEW mvname AS SELECT id, name FROM test");
	        
	        //Original values in the underlying table
	        rs = stat.executeQuery("select * from mvname");
	        rs.next();
	        assertEquals(rs.getString("id"), "1");	        
	        assertEquals(rs.getString("name"), "Hello");
	        rs.next();
	        assertEquals(rs.getString("id"), "2");	        
	        assertEquals(rs.getString("name"), "World");
	        rs.next();
	        assertEquals(rs.getString("id"), "3");	        
	        assertEquals(rs.getString("name"), "!!!");
	        
	        
	        //Insert Values into underlying table after creating the materialized view
	        stat.execute("INSERT INTO test VALUES(4, 'CSC')");
	        stat.execute("INSERT INTO test VALUES(5, '468')");
	        stat.execute("INSERT INTO test VALUES(6, 'DBMS')");
	        rs = stat.executeQuery("select * from mvname");

	        //Check Results
	        rs.next();
	        assertEquals(rs.getString("id"), "1");	        
	        assertEquals(rs.getString("name"), "Hello");
	        rs.next();
	        assertEquals(rs.getString("id"), "2");	        
	        assertEquals(rs.getString("name"), "World");
	        rs.next();
	        assertEquals(rs.getString("id"), "3");	        
	        assertEquals(rs.getString("name"), "!!!");
	        rs.next();
	        assertEquals(rs.getString("id"), "4");	        
	        assertEquals(rs.getString("name"), "CSC");
	        rs.next();
	        assertEquals(rs.getString("id"), "5");	        
	        assertEquals(rs.getString("name"), "468");
	        rs.next();
	        assertEquals(rs.getString("id"), "6");	        
	        assertEquals(rs.getString("name"), "DBMS");
	        

	        stat.close();
	        conn.close();
	        
	  }
	  
	  @Test
	  public void MVTest5Columns() throws ClassNotFoundException, SQLException {
		  
	        // delete the database named 'test' in the user home directory
	        DeleteDbFiles.execute("~", "test", true);

	        Class.forName("org.h2.Driver");
	        Connection conn = DriverManager.getConnection("jdbc:h2:~/test");
	        Statement stat = conn.createStatement();		  
	        ResultSet rs;
	        
	        //
	        //Test with five columns
	        //

	        stat.execute("CREATE TABLE employee(id int primary key, name varchar(255), lastName varchar(255), "
	        		+ "department varchar(255), city varchar(255))");
	        stat.execute("INSERT INTO employee VALUES(1, 'Brian', 'Gomez', 'CSC', 'SLO')");
	        stat.execute("INSERT INTO employee VALUES(2, 'Peter', 'Rodgers', 'MATH', 'Lompoc')");
	        stat.execute("INSERT INTO employee VALUES(3, 'John', 'Schmidt', 'CSC', 'Pismo')");
	        
	        //Create materialize view
	        stat.execute("CREATE MATERIALIZED VIEW mvEmp AS SELECT id, name, lastName, department, city FROM employee");
	        
	        rs = stat.executeQuery("select * from mvEmp");
	        rs.next();
	        assertEquals(rs.getString("id"), "1");	        
	        assertEquals(rs.getString("name"), "Brian");
	        assertEquals(rs.getString("lastName"), "Gomez");
	        assertEquals(rs.getString("department"), "CSC");
	        assertEquals(rs.getString("city"), "SLO");
	        
	        rs.next();
	        assertEquals(rs.getString("id"), "2");	        
	        assertEquals(rs.getString("name"), "Peter");
	        assertEquals(rs.getString("lastName"), "Rodgers");
	        assertEquals(rs.getString("department"), "MATH");
	        assertEquals(rs.getString("city"), "Lompoc");
	        
	        rs.next();
	        assertEquals(rs.getString("id"), "3");	        
	        assertEquals(rs.getString("name"), "John");
	        assertEquals(rs.getString("lastName"), "Schmidt");
	        assertEquals(rs.getString("department"), "CSC");
	        assertEquals(rs.getString("city"), "Pismo");
	
	        
	        
	        
	        //Insert values to underlying table after materialized view was created
	        stat.execute("INSERT INTO employee VALUES(4, 'Albert', 'Strong', 'CPE', 'Santa Maria')");
	        stat.execute("INSERT INTO employee VALUES(5, 'Adam', 'Fisher', 'ENG', 'Paso Robles')");

	        rs = stat.executeQuery("select * from mvEmp");
	        
	        rs.next();
	        rs.next();
	        rs.next();
	        rs.next();
	        assertEquals(rs.getString("id"), "4");	        
	        assertEquals(rs.getString("name"), "Albert");
	        assertEquals(rs.getString("lastName"), "Strong");
	        assertEquals(rs.getString("department"), "CPE");
	        assertEquals(rs.getString("city"), "Santa Maria");
	        
	        rs.next();
	        assertEquals(rs.getString("id"), "5");	        
	        assertEquals(rs.getString("name"), "Adam");
	        assertEquals(rs.getString("lastName"), "Fisher");
	        assertEquals(rs.getString("department"), "ENG");
	        assertEquals(rs.getString("city"), "Paso Robles");
	        

	        
	       
	        stat.close();
	        conn.close();
	        
	  }
	
}
