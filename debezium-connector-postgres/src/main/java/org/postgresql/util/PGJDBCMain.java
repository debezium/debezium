package org.postgresql.util;

public class PGJDBCMain {

  public static void main(String[] args) {

    PSQLDriverVersion.main(args);

    System.out.println("\nThe PgJDBC driver is not an executable Java program.\n\n"
        + "You must install it according to the JDBC driver installation "
        + "instructions for your application / container / appserver, "
        + "then use it by specifying a JDBC URL of the form \n" + "    jdbc:postgresql://\n"
        + "or using an application specific method.\n\n"
        + "See the PgJDBC documentation: http://jdbc.postgresql.org/documentation/head/index.html\n\n"
        + "This command has had no effect.\n");

    System.exit(1);
  }
}
