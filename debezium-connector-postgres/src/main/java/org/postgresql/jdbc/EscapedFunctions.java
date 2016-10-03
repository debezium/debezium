/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.jdbc;

import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * This class stores supported escaped function
 *
 * @author Xavier Poinsard
 */
public class EscapedFunctions {
  // numeric functions names
  public final static String ABS = "abs";
  public final static String ACOS = "acos";
  public final static String ASIN = "asin";
  public final static String ATAN = "atan";
  public final static String ATAN2 = "atan2";
  public final static String CEILING = "ceiling";
  public final static String COS = "cos";
  public final static String COT = "cot";
  public final static String DEGREES = "degrees";
  public final static String EXP = "exp";
  public final static String FLOOR = "floor";
  public final static String LOG = "log";
  public final static String LOG10 = "log10";
  public final static String MOD = "mod";
  public final static String PI = "pi";
  public final static String POWER = "power";
  public final static String RADIANS = "radians";
  public final static String ROUND = "round";
  public final static String SIGN = "sign";
  public final static String SIN = "sin";
  public final static String SQRT = "sqrt";
  public final static String TAN = "tan";
  public final static String TRUNCATE = "truncate";

  // string function names
  public final static String ASCII = "ascii";
  public final static String CHAR = "char";
  public final static String CONCAT = "concat";
  public final static String INSERT = "insert"; // change arguments order
  public final static String LCASE = "lcase";
  public final static String LEFT = "left";
  public final static String LENGTH = "length";
  public final static String LOCATE = "locate"; // the 3 args version duplicate args
  public final static String LTRIM = "ltrim";
  public final static String REPEAT = "repeat";
  public final static String REPLACE = "replace";
  public final static String RIGHT = "right"; // duplicate args
  public final static String RTRIM = "rtrim";
  public final static String SPACE = "space";
  public final static String SUBSTRING = "substring";
  public final static String UCASE = "ucase";
  // soundex is implemented on the server side by
  // the contrib/fuzzystrmatch module. We provide a translation
  // for this in the driver, but since we don't want to bother with run
  // time detection of this module's installation we don't report this
  // method as supported in DatabaseMetaData.
  // difference is currently unsupported entirely.

  // date time function names
  public final static String CURDATE = "curdate";
  public final static String CURTIME = "curtime";
  public final static String DAYNAME = "dayname";
  public final static String DAYOFMONTH = "dayofmonth";
  public final static String DAYOFWEEK = "dayofweek";
  public final static String DAYOFYEAR = "dayofyear";
  public final static String HOUR = "hour";
  public final static String MINUTE = "minute";
  public final static String MONTH = "month";
  public final static String MONTHNAME = "monthname";
  public final static String NOW = "now";
  public final static String QUARTER = "quarter";
  public final static String SECOND = "second";
  public final static String WEEK = "week";
  public final static String YEAR = "year";
  // for timestampadd and timestampdiff the fractional part of second is not supported
  // by the backend
  // timestampdiff is very partially supported
  public final static String TIMESTAMPADD = "timestampadd";
  public final static String TIMESTAMPDIFF = "timestampdiff";

  // constants for timestampadd and timestampdiff
  public final static String SQL_TSI_ROOT = "SQL_TSI_";
  public final static String SQL_TSI_DAY = "DAY";
  public final static String SQL_TSI_FRAC_SECOND = "FRAC_SECOND";
  public final static String SQL_TSI_HOUR = "HOUR";
  public final static String SQL_TSI_MINUTE = "MINUTE";
  public final static String SQL_TSI_MONTH = "MONTH";
  public final static String SQL_TSI_QUARTER = "QUARTER";
  public final static String SQL_TSI_SECOND = "SECOND";
  public final static String SQL_TSI_WEEK = "WEEK";
  public final static String SQL_TSI_YEAR = "YEAR";


  // system functions
  public final static String DATABASE = "database";
  public final static String IFNULL = "ifnull";
  public final static String USER = "user";


  /**
   * storage for functions implementations
   */
  private static Map<String, Method> functionMap = createFunctionMap();

  private static Map<String, Method> createFunctionMap() {
    Method[] arrayMeths = EscapedFunctions.class.getDeclaredMethods();
    Map<String, Method> functionMap = new HashMap<String, Method>(arrayMeths.length * 2);
    for (Method meth : arrayMeths) {
      if (meth.getName().startsWith("sql")) {
        functionMap.put(meth.getName().toLowerCase(Locale.US), meth);
      }
    }
    return functionMap;
  }

  /**
   * get Method object implementing the given function
   *
   * @param functionName name of the searched function
   * @return a Method object or null if not found
   */
  public static Method getFunction(String functionName) {
    return functionMap.get("sql" + functionName.toLowerCase(Locale.US));
  }

  // ** numeric functions translations **

  /**
   * ceiling to ceil translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqlceiling(List<?> parsedArgs) throws SQLException {
    return singleArgumentFunctionCall("ceil(", "ceiling", parsedArgs);
  }

  /**
   * log to ln translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqllog(List<?> parsedArgs) throws SQLException {
    return singleArgumentFunctionCall("ln(", "log", parsedArgs);
  }

  /**
   * log10 to log translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqllog10(List<?> parsedArgs) throws SQLException {
    return singleArgumentFunctionCall("log(", "log10", parsedArgs);
  }

  /**
   * power to pow translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqlpower(List<?> parsedArgs) throws SQLException {
    return twoArgumentsFunctionCall("pow(", "power", parsedArgs);
  }

  /**
   * truncate to trunc translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqltruncate(List<?> parsedArgs) throws SQLException {
    return twoArgumentsFunctionCall("trunc(", "truncate", parsedArgs);
  }

  // ** string functions translations **

  /**
   * char to chr translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqlchar(List<?> parsedArgs) throws SQLException {
    return singleArgumentFunctionCall("chr(", "char", parsedArgs);
  }

  /**
   * concat translation
   *
   * @param parsedArgs arguments
   * @return sql call
   */
  public static String sqlconcat(List<?> parsedArgs) {
    StringBuilder buf = new StringBuilder();
    buf.append('(');
    for (int iArg = 0; iArg < parsedArgs.size(); iArg++) {
      buf.append(parsedArgs.get(iArg));
      if (iArg != (parsedArgs.size() - 1)) {
        buf.append(" || ");
      }
    }
    return buf.append(')').toString();
  }

  /**
   * insert to overlay translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqlinsert(List<?> parsedArgs) throws SQLException {
    StringBuilder buf = new StringBuilder();
    buf.append("overlay(");
    if (parsedArgs.size() != 4) {
      throw new PSQLException(GT.tr("{0} function takes four and only four argument.", "insert"),
          PSQLState.SYNTAX_ERROR);
    }
    buf.append(parsedArgs.get(0)).append(" placing ").append(parsedArgs.get(3));
    buf.append(" from ").append(parsedArgs.get(1)).append(" for ").append(parsedArgs.get(2));
    return buf.append(')').toString();
  }

  /**
   * lcase to lower translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqllcase(List<?> parsedArgs) throws SQLException {
    return singleArgumentFunctionCall("lower(", "lcase", parsedArgs);
  }

  /**
   * left to substring translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqlleft(List<?> parsedArgs) throws SQLException {
    StringBuilder buf = new StringBuilder();
    buf.append("substring(");
    if (parsedArgs.size() != 2) {
      throw new PSQLException(GT.tr("{0} function takes two and only two arguments.", "left"),
          PSQLState.SYNTAX_ERROR);
    }
    buf.append(parsedArgs.get(0)).append(" for ").append(parsedArgs.get(1));
    return buf.append(')').toString();
  }

  /**
   * length translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqllength(List<?> parsedArgs) throws SQLException {
    StringBuilder buf = new StringBuilder();
    buf.append("length(trim(trailing from ");
    if (parsedArgs.size() != 1) {
      throw new PSQLException(GT.tr("{0} function takes one and only one argument.", "length"),
          PSQLState.SYNTAX_ERROR);
    }
    buf.append(parsedArgs.get(0));
    return buf.append("))").toString();
  }

  /**
   * locate translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqllocate(List<?> parsedArgs) throws SQLException {
    if (parsedArgs.size() == 2) {
      return "position(" + parsedArgs.get(0) + " in " + parsedArgs.get(1) + ")";
    } else if (parsedArgs.size() == 3) {
      String tmp = "position(" + parsedArgs.get(0) + " in substring(" + parsedArgs.get(1) + " from "
          + parsedArgs.get(2) + "))";
      return "(" + parsedArgs.get(2) + "*sign(" + tmp + ")+" + tmp + ")";
    } else {
      throw new PSQLException(GT.tr("{0} function takes two or three arguments.", "locate"),
          PSQLState.SYNTAX_ERROR);
    }
  }

  /**
   * ltrim translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqlltrim(List<?> parsedArgs) throws SQLException {
    StringBuilder buf = new StringBuilder();
    buf.append("trim(leading from ");
    if (parsedArgs.size() != 1) {
      throw new PSQLException(GT.tr("{0} function takes one and only one argument.", "ltrim"),
          PSQLState.SYNTAX_ERROR);
    }
    buf.append(parsedArgs.get(0));
    return buf.append(')').toString();
  }

  /**
   * right to substring translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqlright(List<?> parsedArgs) throws SQLException {
    StringBuilder buf = new StringBuilder();
    buf.append("substring(");
    if (parsedArgs.size() != 2) {
      throw new PSQLException(GT.tr("{0} function takes two and only two arguments.", "right"),
          PSQLState.SYNTAX_ERROR);
    }
    buf.append(parsedArgs.get(0))
        .append(" from (length(")
        .append(parsedArgs.get(0))
        .append(")+1-")
        .append(parsedArgs.get(1));
    return buf.append("))").toString();
  }

  /**
   * rtrim translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqlrtrim(List<?> parsedArgs) throws SQLException {
    return singleArgumentFunctionCall("trim(trailing from ", "rtrim", parsedArgs);
  }

  /**
   * space translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqlspace(List<?> parsedArgs) throws SQLException {
    return singleArgumentFunctionCall("repeat(' ',", "space", parsedArgs);
  }

  /**
   * substring to substr translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqlsubstring(List<?> parsedArgs) throws SQLException {
    if (parsedArgs.size() == 2) {
      return "substr(" + parsedArgs.get(0) + "," + parsedArgs.get(1) + ")";
    } else if (parsedArgs.size() == 3) {
      return "substr(" + parsedArgs.get(0) + "," + parsedArgs.get(1) + "," + parsedArgs.get(2)
          + ")";
    } else {
      throw new PSQLException(GT.tr("{0} function takes two or three arguments.", "substring"),
          PSQLState.SYNTAX_ERROR);
    }
  }

  /**
   * ucase to upper translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqlucase(List<?> parsedArgs) throws SQLException {
    return singleArgumentFunctionCall("upper(", "ucase", parsedArgs);
  }

  /**
   * curdate to current_date translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqlcurdate(List<?> parsedArgs) throws SQLException {
    if (!parsedArgs.isEmpty()) {
      throw new PSQLException(GT.tr("{0} function doesn''t take any argument.", "curdate"),
          PSQLState.SYNTAX_ERROR);
    }
    return "current_date";
  }

  /**
   * curtime to current_time translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqlcurtime(List<?> parsedArgs) throws SQLException {
    if (!parsedArgs.isEmpty()) {
      throw new PSQLException(GT.tr("{0} function doesn''t take any argument.", "curtime"),
          PSQLState.SYNTAX_ERROR);
    }
    return "current_time";
  }

  /**
   * dayname translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqldayname(List<?> parsedArgs) throws SQLException {
    if (parsedArgs.size() != 1) {
      throw new PSQLException(GT.tr("{0} function takes one and only one argument.", "dayname"),
          PSQLState.SYNTAX_ERROR);
    }
    return "to_char(" + parsedArgs.get(0) + ",'Day')";
  }

  /**
   * dayofmonth translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqldayofmonth(List<?> parsedArgs) throws SQLException {
    if (parsedArgs.size() != 1) {
      throw new PSQLException(GT.tr("{0} function takes one and only one argument.", "dayofmonth"),
          PSQLState.SYNTAX_ERROR);
    }
    return "extract(day from " + parsedArgs.get(0) + ")";
  }

  /**
   * dayofweek translation adding 1 to postgresql function since we expect values from 1 to 7
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqldayofweek(List<?> parsedArgs) throws SQLException {
    if (parsedArgs.size() != 1) {
      throw new PSQLException(GT.tr("{0} function takes one and only one argument.", "dayofweek"),
          PSQLState.SYNTAX_ERROR);
    }
    return "extract(dow from " + parsedArgs.get(0) + ")+1";
  }

  /**
   * dayofyear translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqldayofyear(List<?> parsedArgs) throws SQLException {
    if (parsedArgs.size() != 1) {
      throw new PSQLException(GT.tr("{0} function takes one and only one argument.", "dayofyear"),
          PSQLState.SYNTAX_ERROR);
    }
    return "extract(doy from " + parsedArgs.get(0) + ")";
  }

  /**
   * hour translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqlhour(List<?> parsedArgs) throws SQLException {
    if (parsedArgs.size() != 1) {
      throw new PSQLException(GT.tr("{0} function takes one and only one argument.", "hour"),
          PSQLState.SYNTAX_ERROR);
    }
    return "extract(hour from " + parsedArgs.get(0) + ")";
  }

  /**
   * minute translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqlminute(List<?> parsedArgs) throws SQLException {
    if (parsedArgs.size() != 1) {
      throw new PSQLException(GT.tr("{0} function takes one and only one argument.", "minute"),
          PSQLState.SYNTAX_ERROR);
    }
    return "extract(minute from " + parsedArgs.get(0) + ")";
  }

  /**
   * month translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqlmonth(List<?> parsedArgs) throws SQLException {
    if (parsedArgs.size() != 1) {
      throw new PSQLException(GT.tr("{0} function takes one and only one argument.", "month"),
          PSQLState.SYNTAX_ERROR);
    }
    return "extract(month from " + parsedArgs.get(0) + ")";
  }

  /**
   * monthname translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqlmonthname(List<?> parsedArgs) throws SQLException {
    if (parsedArgs.size() != 1) {
      throw new PSQLException(GT.tr("{0} function takes one and only one argument.", "monthname"),
          PSQLState.SYNTAX_ERROR);
    }
    return "to_char(" + parsedArgs.get(0) + ",'Month')";
  }

  /**
   * quarter translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqlquarter(List<?> parsedArgs) throws SQLException {
    if (parsedArgs.size() != 1) {
      throw new PSQLException(GT.tr("{0} function takes one and only one argument.", "quarter"),
          PSQLState.SYNTAX_ERROR);
    }
    return "extract(quarter from " + parsedArgs.get(0) + ")";
  }

  /**
   * second translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqlsecond(List<?> parsedArgs) throws SQLException {
    if (parsedArgs.size() != 1) {
      throw new PSQLException(GT.tr("{0} function takes one and only one argument.", "second"),
          PSQLState.SYNTAX_ERROR);
    }
    return "extract(second from " + parsedArgs.get(0) + ")";
  }

  /**
   * week translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqlweek(List<?> parsedArgs) throws SQLException {
    if (parsedArgs.size() != 1) {
      throw new PSQLException(GT.tr("{0} function takes one and only one argument.", "week"),
          PSQLState.SYNTAX_ERROR);
    }
    return "extract(week from " + parsedArgs.get(0) + ")";
  }

  /**
   * year translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqlyear(List<?> parsedArgs) throws SQLException {
    if (parsedArgs.size() != 1) {
      throw new PSQLException(GT.tr("{0} function takes one and only one argument.", "year"),
          PSQLState.SYNTAX_ERROR);
    }
    return "extract(year from " + parsedArgs.get(0) + ")";
  }

  /**
   * time stamp add
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqltimestampadd(List<?> parsedArgs) throws SQLException {
    if (parsedArgs.size() != 3) {
      throw new PSQLException(
          GT.tr("{0} function takes three and only three arguments.", "timestampadd"),
          PSQLState.SYNTAX_ERROR);
    }
    String interval = EscapedFunctions.constantToInterval(parsedArgs.get(0).toString(),
        parsedArgs.get(1).toString());
    StringBuilder buf = new StringBuilder();
    buf.append("(").append(interval).append("+");
    buf.append(parsedArgs.get(2)).append(")");
    return buf.toString();
  }

  private final static String constantToInterval(String type, String value) throws SQLException {
    if (!type.startsWith(SQL_TSI_ROOT)) {
      throw new PSQLException(GT.tr("Interval {0} not yet implemented", type),
          PSQLState.SYNTAX_ERROR);
    }
    String shortType = type.substring(SQL_TSI_ROOT.length());
    if (SQL_TSI_DAY.equalsIgnoreCase(shortType)) {
      return "CAST(" + value + " || ' day' as interval)";
    } else if (SQL_TSI_SECOND.equalsIgnoreCase(shortType)) {
      return "CAST(" + value + " || ' second' as interval)";
    } else if (SQL_TSI_HOUR.equalsIgnoreCase(shortType)) {
      return "CAST(" + value + " || ' hour' as interval)";
    } else if (SQL_TSI_MINUTE.equalsIgnoreCase(shortType)) {
      return "CAST(" + value + " || ' minute' as interval)";
    } else if (SQL_TSI_MONTH.equalsIgnoreCase(shortType)) {
      return "CAST(" + value + " || ' month' as interval)";
    } else if (SQL_TSI_QUARTER.equalsIgnoreCase(shortType)) {
      return "CAST((" + value + "::int * 3) || ' month' as interval)";
    } else if (SQL_TSI_WEEK.equalsIgnoreCase(shortType)) {
      return "CAST(" + value + " || ' week' as interval)";
    } else if (SQL_TSI_YEAR.equalsIgnoreCase(shortType)) {
      return "CAST(" + value + " || ' year' as interval)";
    } else if (SQL_TSI_FRAC_SECOND.equalsIgnoreCase(shortType)) {
      throw new PSQLException(GT.tr("Interval {0} not yet implemented", "SQL_TSI_FRAC_SECOND"),
          PSQLState.SYNTAX_ERROR);
    } else {
      throw new PSQLException(GT.tr("Interval {0} not yet implemented", type),
          PSQLState.SYNTAX_ERROR);
    }
  }


  /**
   * time stamp diff
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqltimestampdiff(List<?> parsedArgs) throws SQLException {
    if (parsedArgs.size() != 3) {
      throw new PSQLException(
          GT.tr("{0} function takes three and only three arguments.", "timestampdiff"),
          PSQLState.SYNTAX_ERROR);
    }
    String datePart = EscapedFunctions.constantToDatePart(parsedArgs.get(0).toString());
    StringBuilder buf = new StringBuilder();
    buf.append("extract( ")
        .append(datePart)
        .append(" from (")
        .append(parsedArgs.get(2))
        .append("-")
        .append(parsedArgs.get(1))
        .append("))");
    return buf.toString();
  }

  private static String constantToDatePart(String type) throws SQLException {
    if (!type.startsWith(SQL_TSI_ROOT)) {
      throw new PSQLException(GT.tr("Interval {0} not yet implemented", type),
          PSQLState.SYNTAX_ERROR);
    }
    String shortType = type.substring(SQL_TSI_ROOT.length());
    if (SQL_TSI_DAY.equalsIgnoreCase(shortType)) {
      return "day";
    } else if (SQL_TSI_SECOND.equalsIgnoreCase(shortType)) {
      return "second";
    } else if (SQL_TSI_HOUR.equalsIgnoreCase(shortType)) {
      return "hour";
    } else if (SQL_TSI_MINUTE.equalsIgnoreCase(shortType)) {
      return "minute";
    } else if (SQL_TSI_FRAC_SECOND.equalsIgnoreCase(shortType)) {
      throw new PSQLException(GT.tr("Interval {0} not yet implemented", "SQL_TSI_FRAC_SECOND"),
          PSQLState.SYNTAX_ERROR);
    } else {
      throw new PSQLException(GT.tr("Interval {0} not yet implemented", type),
          PSQLState.SYNTAX_ERROR);
    }
    // See http://archives.postgresql.org/pgsql-jdbc/2006-03/msg00096.php
    /*
     * else if (SQL_TSI_MONTH.equalsIgnoreCase(shortType)) return "month"; else if
     * (SQL_TSI_QUARTER.equalsIgnoreCase(shortType)) return "quarter"; else if
     * (SQL_TSI_WEEK.equalsIgnoreCase(shortType)) return "week"; else if
     * (SQL_TSI_YEAR.equalsIgnoreCase(shortType)) return "year";
     */
  }

  /**
   * database translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqldatabase(List<?> parsedArgs) throws SQLException {
    if (!parsedArgs.isEmpty()) {
      throw new PSQLException(GT.tr("{0} function doesn''t take any argument.", "database"),
          PSQLState.SYNTAX_ERROR);
    }
    return "current_database()";
  }

  /**
   * ifnull translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqlifnull(List<?> parsedArgs) throws SQLException {
    if (parsedArgs.size() != 2) {
      throw new PSQLException(GT.tr("{0} function takes two and only two arguments.", "ifnull"),
          PSQLState.SYNTAX_ERROR);
    }
    return "coalesce(" + parsedArgs.get(0) + "," + parsedArgs.get(1) + ")";
  }

  /**
   * user translation
   *
   * @param parsedArgs arguments
   * @return sql call
   * @throws SQLException if something wrong happens
   */
  public static String sqluser(List<?> parsedArgs) throws SQLException {
    if (!parsedArgs.isEmpty()) {
      throw new PSQLException(GT.tr("{0} function doesn''t take any argument.", "user"),
          PSQLState.SYNTAX_ERROR);
    }
    return "user";
  }

  private static String singleArgumentFunctionCall(String call, String functionName,
      List<?> parsedArgs) throws PSQLException {
    StringBuilder buf = new StringBuilder();
    buf.append(call);
    if (parsedArgs.size() != 1) {
      throw new PSQLException(GT.tr("{0} function takes one and only one argument.", functionName),
          PSQLState.SYNTAX_ERROR);
    }
    buf.append(parsedArgs.get(0));
    return buf.append(')').toString();
  }

  private static String twoArgumentsFunctionCall(String call, String functionName,
      List<?> parsedArgs) throws PSQLException {
    StringBuilder buf = new StringBuilder();
    buf.append(call);
    if (parsedArgs.size() != 2) {
      throw new PSQLException(GT.tr("{0} function takes two and only two arguments.", functionName),
          PSQLState.SYNTAX_ERROR);
    }
    buf.append(parsedArgs.get(0)).append(',').append(parsedArgs.get(1));
    return buf.append(')').toString();
  }
}
