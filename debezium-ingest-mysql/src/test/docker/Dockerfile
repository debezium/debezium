FROM mysql:5.7

MAINTAINER Debezium Community

COPY * /docker-entrypoint-initdb.d/
RUN rm /docker-entrypoint-initdb.d/Dockerfile && \
    chmod 755 *.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["mysqld"]
