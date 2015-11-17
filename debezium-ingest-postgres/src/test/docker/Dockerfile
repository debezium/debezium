FROM postgres:9.4

MAINTAINER Debezium Community

COPY * /docker-entrypoint-initdb.d/
RUN rm /docker-entrypoint-initdb.d/Dockerfile && \
    chmod 755 *.sh

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["postgres"]
