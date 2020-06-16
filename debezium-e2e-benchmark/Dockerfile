FROM centos:8


RUN dnf -y install java-1.8.0-openjdk java-1.8.0-openjdk-devel
RUN dnf -y install python3
RUN dnf -y install gcc gcc-c++  python3-devel python3-requests
RUN python3 -m pip install JPype1==0.6.3
RUN python3 -m pip install JayDeBeApi matplotlib kafka-python scipy

RUN useradd -ms /bin/bash tpc

USER tpc
WORKDIR /home/tpc

RUN mkdir /home/tpc/jdbcdriver
run curl https://repo1.maven.org/maven2/com/ibm/db2/jcc/11.5.0.0/jcc-11.5.0.0.jar --output /home/tpc/jdbcdriver/jcc.jar
run curl https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar --output /home/tpc/jdbcdriver/mysql.jar
run curl https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/8.2.0.jre8/mssql-jdbc-8.2.0.jre8.jar --output /home/tpc/jdbcdriver/mssql.jar
run curl https://repo1.maven.org/maven2/postgresql/postgresql/9.1-901.jdbc4/postgresql-9.1-901.jdbc4.jar --output /home/tpc/jdbcdriver/postgresql.jar
run curl https://repo1.maven.org/maven2/com/oracle/ojdbc/ojdbc10/19.3.0.0/ojdbc10-19.3.0.0.jar  --output /home/tpc/jdbcdriver/ojdbc10.jar

ADD py/ /home/tpc

CMD ["/usr/bin/tail","-f","/dev/null"]
