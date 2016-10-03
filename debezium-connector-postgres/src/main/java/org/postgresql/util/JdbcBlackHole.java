package org.postgresql.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JdbcBlackHole {
  public static void close(Connection con) {
    try {
      if (con != null) {
        con.close();
      }
    } catch (SQLException e) {
      /* ignore for now */
    }
  }

  public static void close(Statement s) {
    try {
      if (s != null) {
        s.close();
      }
    } catch (SQLException e) {
      /* ignore for now */
    }
  }

  public static void close(ResultSet rs) {
    try {
      if (rs != null) {
        rs.close();
      }
    } catch (SQLException e) {
      /* ignore for now */
    }
  }
}
