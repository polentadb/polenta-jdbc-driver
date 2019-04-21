package com.polenta.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.Test;

public class PolentaTest {

	@Test
	public void test() throws ClassNotFoundException, SQLException {

		Class.forName("com.polenta.jdbc.PolentaDriver");

		Connection conn = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", "");
		connectionProps.put("password", "");

		conn = DriverManager.getConnection("jdbc:polenta://localhost:3110/", connectionProps);

		if (conn != null) {
			Statement stmt = conn.createStatement();
			//stmt.executeQuery("CREATE TABLE");
			stmt.execute("CREATE BAG PERSON (NAME STRING)");
			
			ResultSet rs = stmt.executeQuery("SELECT * FROM PERSON");
			
			//rs.first();
			
		}

	}

}
