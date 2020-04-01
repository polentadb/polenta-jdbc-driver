package com.polenta.jdbc;

import java.sql.*;
import java.util.List;
import java.util.Map;

public class PolentaStatement implements Statement {
	
	private PolentaConnection connection;
	
	PolentaStatement(PolentaConnection connection) {
		this.connection = connection;
	}

	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");		
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");		
	}

	@SuppressWarnings("unchecked")
	public ResultSet executeQuery(String sql) throws SQLException {
		if (!connection.isClosed()) {
			Map<String, Object> serverResponse = connection.writeToSocket(sql);
			if (!serverResponse.containsKey("STATUS")) {
				throw new SQLException("Received no status from server.");
			} else if (serverResponse.get("STATUS").equals("ERROR")) {
				if (!serverResponse.containsKey("ERROR_MESSAGE")) {
					throw new SQLException("Received empty error message from server.");
				} else {
					throw new SQLException((String)serverResponse.get("ERROR_MESSAGE"));
				}
			} else if (!serverResponse.get("STATUS").equals("SUCCESS")) {
				throw new SQLException("Received unexpected status from server.");
			} else {
				if (!serverResponse.containsKey("FIELDS") && !serverResponse.containsKey("ROWS")) {
					throw new SQLException("Received no result set from server.");
				} else {
					return new PolentaResultSet((List<String>)serverResponse.get("FIELDS"), (List<Map<String, Object>>)serverResponse.get("ROWS"));
				}
			}
		} else {
			throw new SQLException("Connection to server has been closed. Statement cannot be executed"); 
		}
	}

	public int executeUpdate(String sql) throws SQLException {
		if (!connection.isClosed()) {
			Map<String, Object> serverResponse = connection.writeToSocket(sql);
			if (!serverResponse.containsKey("STATUS")) {
				throw new SQLException("Received no status from server.");
			} else if (serverResponse.get("STATUS").equals("ERROR")) {
				if (!serverResponse.containsKey("ERROR_MESSAGE")) {
					throw new SQLException("Received empty error message from server.");
				} else {
					throw new SQLException((String)serverResponse.get("ERROR_MESSAGE"));
				}
			} else if (!serverResponse.get("STATUS").equals("SUCCESS")) {
				throw new SQLException("Received unexpected status from server. Status: " + serverResponse.get("STATUS"));
			} else {
				return 1;
			}
		} else {
			throw new SQLException("Connection to server has been closed. Statement cannot be executed"); 
		}	
	}

	public void close() throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public int getMaxFieldSize() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public void setMaxFieldSize(int max) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public int getMaxRows() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public void setMaxRows(int max) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void setEscapeProcessing(boolean enable) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public int getQueryTimeout() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public void setQueryTimeout(int seconds) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void cancel() throws SQLException {
		throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");		
	}

	public SQLWarning getWarnings() throws SQLException {
		throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");		
	}

	public void clearWarnings() throws SQLException {
		throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");		
	}

	public void setCursorName(String name) throws SQLException {
		throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public boolean execute(String sql) throws SQLException {
		if (!connection.isClosed()) {
			Map<String, Object> serverResponse = connection.writeToSocket(sql);
			if (!serverResponse.containsKey("STATUS")) {
				throw new SQLException("Received no status from server.");
			} else if (serverResponse.get("STATUS").equals("ERROR")) {
				if (!serverResponse.containsKey("ERROR_MESSAGE")) {
					throw new SQLException("Received empty error message from server.");
				} else {
					throw new SQLException((String)serverResponse.get("ERROR_MESSAGE"));
				}
			} else if (!serverResponse.get("STATUS").equals("SUCCESS")) {
				throw new SQLException("Received unexpected status from server.");
			} else {
				return true;
			}
		} else {
			throw new SQLException("Connection to server has been closed. Statement cannot be executed"); 
		}		
	}

	public ResultSet getResultSet() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public int getUpdateCount() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean getMoreResults() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public void setFetchDirection(int direction) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public int getFetchDirection() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public void setFetchSize(int rows) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public int getFetchSize() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public int getResultSetConcurrency() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public int getResultSetType() throws SQLException {
		// TODO Auto-generated method stub
		return 0;
	}

	public void addBatch(String sql) throws SQLException {
		throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");		
	}

	public void clearBatch() throws SQLException {
		throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");		
	}

	public int[] executeBatch() throws SQLException {
		throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");		
	}

	public Connection getConnection() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean getMoreResults(int current) throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public ResultSet getGeneratedKeys() throws SQLException {
		// TODO Auto-generated method stub
		return null;
	}

	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");		
	}

	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");		
	}

	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");		
	}

	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");		
	}

	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");		
	}

	public boolean execute(String sql, String[] columnNames) throws SQLException {
		throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");		
	}

	public int getResultSetHoldability() throws SQLException {
		throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");		
	}

	public boolean isClosed() throws SQLException {
		// TODO Auto-generated method stub
		return false;
	}

	public void setPoolable(boolean poolable) throws SQLException {
		throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");		
	}

	public boolean isPoolable() throws SQLException {
		throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");		
	}

	public void closeOnCompletion() throws SQLException {
		throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");		
	}

	public boolean isCloseOnCompletion() throws SQLException {
		throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");		
	}

}
