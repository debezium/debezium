# In production you would almost certainly limit the replication user must be on the follower (slave) machine,
# to prevent other clients accessing the log from other machines. For example, 'replicator'@'follower.acme.com'.
#
# However, this grant is equivalent to specifying *any* hosts, which makes this easier since the docker host
# is not easily known to the Docker container
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'replicator' IDENTIFIED BY 'replpass';

# Create the database that we'll use to populate data and watch the effect in the binlog
CREATE DATABASE readbinlog_test;
GRANT ALL PRIVILEGES ON readbinlog_test.* TO 'mysqluser'@'%';
