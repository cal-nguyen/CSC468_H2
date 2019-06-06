package org.h2.samples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.h2.tools.DeleteDbFiles;

public class MVUnitTestDisplay {

	
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

        //Create underlying table and insert values
        stat.execute("CREATE TABLE employee(id int primary key, name varchar(255), lastName varchar(255), "
        		+ "department varchar(255), city varchar(255))");
        stat.execute("INSERT INTO employee VALUES(1, 'Brian', 'Gomez', 'CSC', 'SLO')");
        stat.execute("INSERT INTO employee VALUES(2, 'Peter', 'Rodgers', 'MATH', 'Lompoc')");
        stat.execute("INSERT INTO employee VALUES(3, 'John', 'Schmidt', 'CSC', 'Pismo')");
        
        //Create materialize view
        stat.execute("CREATE MATERIALIZED VIEW mvEmp AS SELECT id, name, lastName, department, city FROM employee");
        
        //Display values from materialized view
        rs = stat.executeQuery("select * from mvEmp");
        System.out.println("\n\n\nOriginal Values from underlying table");
        System.out.println("id\tname\tdepartment\tsalary\tage");
        System.out.println("---------------------------------------------");
        while (rs.next()) {
            System.out.print(rs.getString("id"));
            System.out.print("\t" + rs.getString("name"));
            System.out.print("\t" + rs.getString("lastName"));
            System.out.print("\t\t" + rs.getString("department"));
            System.out.print("\t" + rs.getString("city") + "\n");
        }
        
        
        //Insert values to underlying table after materialized view was created
        stat.execute("INSERT INTO employee VALUES(4, 'Albert', 'Strong', 'CPE', 'Santa Maria')");
        stat.execute("INSERT INTO employee VALUES(5, 'Adam', 'Fisher', 'ENG', 'Paso Robles')");

        
        //Display updated values of materialized view
        rs = stat.executeQuery("select * from mvEmp");
        System.out.println("\nValues after underlying table INSERT");
        System.out.println("id\tname\tdepartment\tsalary\tage");
        System.out.println("---------------------------------------------");
        while (rs.next()) {
            System.out.print(rs.getString("id"));
            System.out.print("\t" + rs.getString("name"));
            System.out.print("\t" + rs.getString("lastName"));
            System.out.print("\t\t" + rs.getString("department"));
            System.out.print("\t" + rs.getString("city") + "\n");
        }
        
       
        stat.close();
        conn.close();
    }
	
	
	
}
