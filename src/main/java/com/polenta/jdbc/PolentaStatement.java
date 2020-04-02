package com.polenta.jdbc;

import com.polenta.jdbc.exception.MethodNotSupportedSQLException;

import java.sql.*;
import java.util.List;
import java.util.Map;

public class PolentaStatement implements Statement {
	
	private PolentaConnection connection;
	
	PolentaStatement(PolentaConnection connection) {
		this.connection = connection;
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

	/******************************************************************************************************/
	/***************** METHODS REQUIRED BY INTERFACE BUT NOT YET SUPPORTED BY DRIVER **********************/
	/******************************************************************************************************/

	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void close() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public int getMaxFieldSize() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void setMaxFieldSize(int max) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public int getMaxRows() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void setMaxRows(int max) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void setEscapeProcessing(boolean enable) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public int getQueryTimeout() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void setQueryTimeout(int seconds) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void cancel() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public SQLWarning getWarnings() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void clearWarnings() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void setCursorName(String name) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public ResultSet getResultSet() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public int getUpdateCount() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public boolean getMoreResults() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void setFetchDirection(int direction) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public int getFetchDirection() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void setFetchSize(int rows) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public int getFetchSize() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public int getResultSetConcurrency() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public int getResultSetType() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void addBatch(String sql) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void clearBatch() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public int[] executeBatch() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Connection getConnection() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public boolean getMoreResults(int current) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public ResultSet getGeneratedKeys() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public boolean execute(String sql, String[] columnNames) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public int getResultSetHoldability() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public boolean isClosed() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void setPoolable(boolean poolable) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public boolean isPoolable() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void closeOnCompletion() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public boolean isCloseOnCompletion() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

}
