package com.polenta.jdbc;

import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PolentaDriverTest extends Driver {

	@Test
	public void testValidURLFormatAreAccepted() throws SQLException {
		assertTrue(acceptsURL("jdbc:polenta://localhost:3110/"));
	}

	@Test
	public void testInvalidURLFormatAreNotAccepted() throws SQLException {
		assertFalse(acceptsURL(null));
		assertFalse(acceptsURL(""));
		assertFalse(acceptsURL("polenta://localhost:3110/"));
		assertFalse(acceptsURL("jdbc:polenta://localhost:3110"));
		assertFalse(acceptsURL("jdbc:polenta://localhost/"));
		assertFalse(acceptsURL("jdbc:polenta://3110/"));
		assertFalse(acceptsURL("jdbc:polenta://3110:3110/"));
		assertFalse(acceptsURL("jdbc:polenta://3110:localhost/"));
		assertFalse(acceptsURL("jdbc:polenta:///"));
		assertFalse(acceptsURL("jdbc:polenta://localhost:localhost/"));
		assertFalse(acceptsURL("jdbc:polenta://localhost:localhost:3110/"));
	}

}
