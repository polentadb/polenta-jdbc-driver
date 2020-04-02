package com.polenta.jdbc;

import com.polenta.jdbc.exception.MethodNotSupportedSQLException;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

@SuppressWarnings("deprecation")
public class PolentaDriver implements java.sql.Driver {

	static {
		try {
			PolentaDriver driverInst = new PolentaDriver();
			DriverManager.registerDriver(driverInst);
			//System.setSecurityManager(new RMISecurityManager());
		} catch (Exception e) {
			e.printStackTrace();
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

	public int getMajorVersion() {
		return 0;
	}

	public int getMinorVersion() {
		return 0;
	}

	public boolean jdbcCompliant() {
		return true;
	}

	/******************************************************************************************************/
	/***************** METHODS REQUIRED BY INTERFACE BUT NOT YET SUPPORTED BY DRIVER **********************/
	/******************************************************************************************************/

	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw  new SQLFeatureNotSupportedException();
	}

}
