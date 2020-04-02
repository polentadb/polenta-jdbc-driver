package com.polenta.jdbc.exception;

import java.sql.SQLException;

public class EmptyResultSetSQLException extends SQLException {

    public EmptyResultSetSQLException() {
        super("ResultSet is empty");
    }

}
