package com.polenta.jdbc;

import com.polenta.jdbc.exception.EmptyResultSetSQLException;
import com.polenta.jdbc.exception.MethodNotSupportedSQLException;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

public class PolentaResultSet implements ResultSet {

	private int current = -1;
	private List<Map<String, Object>> rows;
	private List<String> fields;
	private int rowCount = -1;

	PolentaResultSet(List<String> fields, List<Map<String, Object>> rows) {
		this.fields = fields;
		this.rows = rows;
		this.rowCount = rows.size();
	}

	public boolean next() throws SQLException {
		checkIfInitialized();
		checkIfEmpty();
		if ((this.current == (this.rowCount - 1))) return false;
		this.current++;
		return true;
	}

	public boolean isBeforeFirst() throws SQLException {
		checkIfInitialized();
		checkIfEmpty();
		return this.current == -1;
	}

	public boolean isAfterLast() throws SQLException {
		checkIfInitialized();
		checkIfEmpty();
		return this.current > (this.rowCount - 1);
	}

	public boolean isFirst() throws SQLException {
		checkIfInitialized();
		checkIfEmpty();
		return this.current == 0;
	}

	public boolean isLast() throws SQLException {
		checkIfInitialized();
		checkIfEmpty();
		return this.current == (this.rowCount - 1);
	}

	public int getInt(int columnIndex) throws SQLException {
		checkIfInitialized();
		checkIfEmpty();
		Object columnValue =  getObject(columnIndex);
		try {
			return ((Integer)columnValue).intValue();
		} catch (Exception e) {
			throw  new SQLException("Failed to convert to int");
		}
	}

	public int getInt(String columnLabel) throws SQLException {
		return getInt(findColumn(columnLabel));
	}

	public String getString(int columnIndex) throws SQLException {
		checkIfInitialized();
		checkIfEmpty();
		Object columnValue =  getObject(columnIndex);
		try {
			return (String)columnValue;
		} catch (Exception e) {
			throw  new SQLException("Failed to convert to int");
		}
	}

	public String getString(String columnLabel) throws SQLException {
		return getString(findColumn(columnLabel));
	}

	public long getLong(int columnIndex) throws SQLException {
		checkIfInitialized();
		checkIfEmpty();
		Object columnValue =  getObject(columnIndex);
		try {
			return ((Long)columnValue).intValue();
		} catch (Exception e) {
			throw  new SQLException("Failed to convert to int");
		}
	}

	public long getLong(String columnLabel) throws SQLException {
		return getLong(findColumn(columnLabel));
	}

	public Object getObject(int columnIndex) throws SQLException {
		checkIfInitialized();
		checkIfEmpty();
		return rows.get(current).get(fields.get(columnIndex));
	}

	public Object getObject(String columnLabel) throws SQLException {
		return getObject(findColumn(columnLabel));
	}

	public int findColumn(String columnLabel) throws SQLException {
		checkIfInitialized();
		checkIfEmpty();
		int columnIndex = fields.indexOf(columnLabel);
		if (columnIndex == -1) {
			throw new SQLException("No such column");
		}
		return columnIndex;
	}

	public boolean first() throws SQLException {
		checkIfInitialized();
		checkIfEmpty();
		this.current = 0;
		return true;
	}

	public boolean last() throws SQLException {
		checkIfInitialized();
		checkIfEmpty();
		this.current = this.rows.size() - 1;
		return true;
	}

	public int getRow() throws SQLException {
		checkIfInitialized();
		checkIfEmpty();
		return current;
	}

	public boolean previous() throws SQLException {
		checkIfInitialized();
		checkIfEmpty();
		if (this.current == 0) return false;
		this.current--;
		return true;
	}

	private void checkIfInitialized() throws SQLException {
		if (this.rowCount == -1) {
			throw new SQLException("ResultSet not initialized.");
		}
	}

	private void checkIfEmpty() throws SQLException {
		if (this.rowCount == 0) {
			throw new EmptyResultSetSQLException();
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

	public boolean wasNull() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public boolean getBoolean(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public byte getByte(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public short getShort(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public float getFloat(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public double getDouble(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public byte[] getBytes(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Date getDate(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Time getTime(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public boolean getBoolean(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public byte getByte(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public short getShort(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public float getFloat(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public double getDouble(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public byte[] getBytes(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Date getDate(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Time getTime(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public SQLWarning getWarnings() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void clearWarnings() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public String getCursorName() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public ResultSetMetaData getMetaData() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Reader getCharacterStream(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Reader getCharacterStream(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void beforeFirst() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void afterLast() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public boolean absolute(int row) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public boolean relative(int rows) throws SQLException {
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

	public int getType() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public int getConcurrency() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public boolean rowUpdated() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public boolean rowInserted() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public boolean rowDeleted() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateNull(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateShort(int columnIndex, short x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateInt(int columnIndex, int x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateLong(int columnIndex, long x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateString(int columnIndex, String x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateNull(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateBoolean(String columnLabel, boolean x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateByte(String columnLabel, byte x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateShort(String columnLabel, short x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateInt(String columnLabel, int x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateLong(String columnLabel, long x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateFloat(String columnLabel, float x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateDouble(String columnLabel, double x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateString(String columnLabel, String x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateDate(String columnLabel, Date x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateTime(String columnLabel, Time x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateObject(String columnLabel, Object x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void insertRow() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateRow() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void deleteRow() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void refreshRow() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void cancelRowUpdates() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void moveToInsertRow() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void moveToCurrentRow() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Statement getStatement() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Ref getRef(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Blob getBlob(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Clob getClob(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Array getArray(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Ref getRef(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Blob getBlob(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Clob getClob(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Array getArray(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public URL getURL(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public URL getURL(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateRef(String columnLabel, Ref x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateClob(String columnLabel, Clob x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateArray(String columnLabel, Array x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public RowId getRowId(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public RowId getRowId(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public int getHoldability() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public boolean isClosed() throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateNString(int columnIndex, String nString) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateNString(String columnLabel, String nString) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public NClob getNClob(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public NClob getNClob(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public String getNString(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public String getNString(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateClob(String columnLabel, Reader reader) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		throw new MethodNotSupportedSQLException();
	}

}
