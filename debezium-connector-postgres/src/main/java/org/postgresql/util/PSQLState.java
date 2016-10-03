/*-------------------------------------------------------------------------
*
* Copyright (c) 2003-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.util;

/**
 * This class is used for holding SQLState codes.
 */
public class PSQLState implements java.io.Serializable {
  private String state;

  public String getState() {
    return this.state;
  }

  public PSQLState(String state) {
    this.state = state;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PSQLState psqlState = (PSQLState) o;
    return !(state != null ? !state.equals(psqlState.state) : psqlState.state != null);
  }

  @Override
  public int hashCode() {
    return state != null ? state.hashCode() : 0;
  }

  // begin constant state codes
  public final static PSQLState UNKNOWN_STATE = new PSQLState("");

  public final static PSQLState TOO_MANY_RESULTS = new PSQLState("0100E");

  public final static PSQLState NO_DATA = new PSQLState("02000");

  public final static PSQLState INVALID_PARAMETER_TYPE = new PSQLState("07006");

  /**
   * We could establish a connection with the server for unknown reasons. Could be a network
   * problem.
   */
  public final static PSQLState CONNECTION_UNABLE_TO_CONNECT = new PSQLState("08001");

  public final static PSQLState CONNECTION_DOES_NOT_EXIST = new PSQLState("08003");

  /**
   * The server rejected our connection attempt. Usually an authentication failure, but could be a
   * configuration error like asking for a SSL connection with a server that wasn't built with SSL
   * support.
   */
  public final static PSQLState CONNECTION_REJECTED = new PSQLState("08004");

  /**
   * After a connection has been established, it went bad.
   */
  public final static PSQLState CONNECTION_FAILURE = new PSQLState("08006");
  public final static PSQLState CONNECTION_FAILURE_DURING_TRANSACTION = new PSQLState("08007");

  /**
   * The server sent us a response the driver was not prepared for and is either bizarre datastream
   * corruption, a driver bug, or a protocol violation on the server's part.
   */
  public final static PSQLState PROTOCOL_VIOLATION = new PSQLState("08P01");

  public final static PSQLState COMMUNICATION_ERROR = new PSQLState("08S01");

  public final static PSQLState NOT_IMPLEMENTED = new PSQLState("0A000");

  public final static PSQLState DATA_ERROR = new PSQLState("22000");
  public final static PSQLState NUMERIC_VALUE_OUT_OF_RANGE = new PSQLState("22003");
  public final static PSQLState BAD_DATETIME_FORMAT = new PSQLState("22007");
  public final static PSQLState DATETIME_OVERFLOW = new PSQLState("22008");
  public final static PSQLState DIVISION_BY_ZERO = new PSQLState("22012");
  public final static PSQLState MOST_SPECIFIC_TYPE_DOES_NOT_MATCH = new PSQLState("2200G");
  public final static PSQLState INVALID_PARAMETER_VALUE = new PSQLState("22023");

  public final static PSQLState INVALID_CURSOR_STATE = new PSQLState("24000");

  public final static PSQLState TRANSACTION_STATE_INVALID = new PSQLState("25000");
  public final static PSQLState ACTIVE_SQL_TRANSACTION = new PSQLState("25001");
  public final static PSQLState NO_ACTIVE_SQL_TRANSACTION = new PSQLState("25P01");
  public final static PSQLState IN_FAILED_SQL_TRANSACTION = new PSQLState("25P02");

  public final static PSQLState INVALID_SQL_STATEMENT_NAME = new PSQLState("26000");
  public final static PSQLState INVALID_AUTHORIZATION_SPECIFICATION = new PSQLState("28000");

  public final static PSQLState STATEMENT_NOT_ALLOWED_IN_FUNCTION_CALL = new PSQLState("2F003");

  public final static PSQLState INVALID_SAVEPOINT_SPECIFICATION = new PSQLState("3B000");

  public final static PSQLState SYNTAX_ERROR = new PSQLState("42601");
  public final static PSQLState UNDEFINED_COLUMN = new PSQLState("42703");
  public final static PSQLState UNDEFINED_OBJECT = new PSQLState("42704");
  public final static PSQLState WRONG_OBJECT_TYPE = new PSQLState("42809");
  public final static PSQLState NUMERIC_CONSTANT_OUT_OF_RANGE = new PSQLState("42820");
  public final static PSQLState DATA_TYPE_MISMATCH = new PSQLState("42821");
  public final static PSQLState UNDEFINED_FUNCTION = new PSQLState("42883");
  public final static PSQLState INVALID_NAME = new PSQLState("42602");

  public final static PSQLState OUT_OF_MEMORY = new PSQLState("53200");
  public final static PSQLState OBJECT_NOT_IN_STATE = new PSQLState("55000");
  public final static PSQLState OBJECT_IN_USE = new PSQLState("55006");


  public final static PSQLState SYSTEM_ERROR = new PSQLState("60000");
  public final static PSQLState IO_ERROR = new PSQLState("58030");

  public final static PSQLState UNEXPECTED_ERROR = new PSQLState("99999");
}
