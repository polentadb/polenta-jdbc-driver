package com.polenta.jdbc;

import com.polenta.jdbc.exception.MethodNotSupportedSQLException;

import java.io.BufferedWriter;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class PolentaConnection implements Connection {

	private String host;
	private int port;
	private Socket socket;
	private boolean connected;
	private BufferedWriter writer;
	private ObjectInputStream reader;

	PolentaConnection(String url, Properties info) throws SQLException {
		String params = url.substring(15, (url.length() - 1));
		String paramsArray[] = params.split(":");

		this.host = paramsArray[0];
		this.port = Integer.parseInt(paramsArray[1]);

		try {
			socket = new Socket(host, port);
			socket.setKeepAlive(true);
			connected = true;
			
			writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
			reader = new ObjectInputStream(socket.getInputStream());
		} catch (java.lang.Exception e) {
			throw new SQLException("It was not possible connect to Polenta server on host [" + this.host + "] and port [" + this.port + "].\n", e);
		}
	}

	@SuppressWarnings("unchecked")
	protected Map<String, Object> writeToSocket(String statement) throws SQLException {
		if (this.connected) {
			Map<String, Object> response = null;
			try {

				writer.write(statement);
				writer.newLine();
				writer.flush();

				response = (Map<String, Object>)reader.readObject();
			} catch (Exception e) {
				throw new SQLException("An error ocurred on communication to PolentaServer. See root cause for details.", e);
			}
			if (response == null) {
				this.connected = false;
				throw new SQLException("Connection to server is no longer active.");
			}
			return response;
		} else {
			throw new SQLException("Connection to server has been closed.");
		}

	}

	public Statement createStatement() {
		return new PolentaStatement(this);
	}

	public void close() throws SQLException {
		this.connected = false;
		try {
			socket.close();
		} catch (Exception e) {
			throw new SQLException("Failed to close", e);
		}
	}

	public boolean isClosed() {
		return !connected;
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

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public CallableStatement prepareCall(String sql) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public String nativeSQL(String sql) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void setAutoCommit(boolean autoCommit) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public boolean getAutoCommit() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void commit() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void rollback() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void setReadOnly(boolean readOnly) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public boolean isReadOnly() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void setCatalog(String catalog) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public String getCatalog() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void setTransactionIsolation(int level) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public int getTransactionIsolation() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public SQLWarning getWarnings() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void clearWarnings() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Map<String, Class<?>> getTypeMap() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void setHoldability(int holdability) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public int getHoldability() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Savepoint setSavepoint() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Savepoint setSavepoint(String name) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void rollback(Savepoint savepoint) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Clob createClob() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Blob createBlob() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public NClob createNClob() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public SQLXML createSQLXML() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public boolean isValid(int timeout) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		throw new SQLClientInfoException();
	}

	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		throw new SQLClientInfoException();
	}

	public String getClientInfo(String name) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Properties getClientInfo() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void setSchema(String schema) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public String getSchema() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void abort(Executor executor) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public int getNetworkTimeout() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

}
