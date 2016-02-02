FROM postgres:9.4

MAINTAINER Debezium Community

#
# Add our scripts to the Docker entry point initialization directory
#
COPY init/* /docker-entrypoint-initdb.d/

#
# Changes to the PostgreSQL configuration are made via the `00-alter-conf.sql` file.
#

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["postgres"]
