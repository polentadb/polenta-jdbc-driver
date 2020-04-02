package com.polenta.jdbc.exception;

import java.sql.SQLException;

public class MethodNotSupportedSQLException extends SQLException {

    public MethodNotSupportedSQLException() {
        super("Method not supported by driver!  Wait for a new version of Polenta JDBC Driver.");
    }

}
