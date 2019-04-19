package com.polenta.jdbc;

import java.io.BufferedWriter;
import java.io.ObjectInputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class PolentaConnection implements Connection {

	//private String url;
	//private Properties info;
	private String host;
	private int port;
	private Socket socket;
	private boolean connected;
	private BufferedWriter writer;
	private ObjectInputStream reader;

	PolentaConnection(String url, Properties info) throws SQLException {
//		String url = url;
//		this.info = info;

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

	public <T> T unwrap(Class<T> iface) throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public Statement createStatement() throws SQLException {
		return null;
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return new PolentaPreparedStatement(sql);
	}

	public CallableStatement prepareCall(String sql) throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public String nativeSQL(String sql) throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public void setAutoCommit(boolean autoCommit) throws SQLException {
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public boolean getAutoCommit() throws SQLException {
		return false;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public void commit() throws SQLException {
		throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public void rollback() throws SQLException {
		throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public void close() throws SQLException {
		this.connected = false;
		try {
			socket.close();	
		} catch (Exception e) {
			// TODO: handle exception
		}
	}

	public boolean isClosed() throws SQLException {
		return !connected;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public void setReadOnly(boolean readOnly) throws SQLException {
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public boolean isReadOnly() throws SQLException {
		return false;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public void setCatalog(String catalog) throws SQLException {
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public String getCatalog() throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public void setTransactionIsolation(int level) throws SQLException {
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public int getTransactionIsolation() throws SQLException {
		return 0;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public SQLWarning getWarnings() throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public void clearWarnings() throws SQLException {
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public void setHoldability(int holdability) throws SQLException {
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public int getHoldability() throws SQLException {
		return 0;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public Savepoint setSavepoint() throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public Savepoint setSavepoint(String name) throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public void rollback(Savepoint savepoint) throws SQLException {
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public Clob createClob() throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public Blob createBlob() throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public NClob createNClob() throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public SQLXML createSQLXML() throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public boolean isValid(int timeout) throws SQLException {
		return false;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		//throw new SQLClientInfoException();
	}

	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		//throw new SQLClientInfoException();
	}

	public String getClientInfo(String name) throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public Properties getClientInfo() throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public void setSchema(String schema) throws SQLException {
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public String getSchema() throws SQLException {
		return null;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public void abort(Executor executor) throws SQLException {
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
	}

	public int getNetworkTimeout() throws SQLException {
		return 0;
		//throw new SQLException("Operation not supported! Wait for a new version of Polenta JDBC Driver!");
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
	
}
