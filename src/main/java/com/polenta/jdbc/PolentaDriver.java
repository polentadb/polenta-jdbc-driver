package com.polenta.jdbc;

import java.rmi.RMISecurityManager;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;

@SuppressWarnings("deprecation")
public class PolentaDriver implements java.sql.Driver {

	static {
		try {
			PolentaDriver driverInst = new PolentaDriver();
			DriverManager.registerDriver(driverInst);
			System.setSecurityManager(new RMISecurityManager());
		} catch (Exception e) {
		}
	}

	public Connection connect(String url, Properties info) throws SQLException {
		Connection conn = null;
		if (acceptsURL(url)) {
			conn = new PolentaConnection(url, info);
		}
		return conn;
	}

	public boolean acceptsURL(String url) throws SQLException {
		if (url == null) return false;
		if (url.equals("")) return false;
		if (!url.startsWith("jdbc:polenta://")) return false;
		if (url.charAt(url.length() - 1) != '/') return false;
		String params = url.substring(15, (url.length() - 1));
		if (params == "") return false;
		String paramsArray[] = params.split(":");
		if (paramsArray.length != 2) return false;
		try {
			Integer.parseInt(paramsArray[0]);
			return false;
		} catch (NumberFormatException e) {
		}
		try {
			Integer.parseInt(paramsArray[1]);
		} catch (NumberFormatException e) {
			return false;
		}
		return true;
	}
	
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		//return null;
		throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public int getMajorVersion() {
		return 0;
	}

	public int getMinorVersion() {
		return 0;
	}

	public boolean jdbcCompliant() {
		return false;
	}

	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return null;
	}

	public static void main(String[] args) throws Exception {

		Class.forName("com.polenta.jdbc.PolentaDriver");
		
		Connection conn = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", "");
		connectionProps.put("password", "");

		conn = DriverManager.getConnection("jdbc:polenta://localhost:3110/", connectionProps);
		
		if (conn != null) {
			Statement stmt = conn.createStatement();
		}

	}

}
