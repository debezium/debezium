FROM mysql/mysql-server:5.7

MAINTAINER Debezium Community

#
# Add our scripts to the Docker entry point initialization directory
#
COPY init/* /docker-entrypoint-initdb.d/

#
# Overwrite the MySQL configuration file with ours
#
COPY mysql.cnf /etc/my.cnf

ENTRYPOINT ["/entrypoint.sh"]
CMD ["mysqld"]
