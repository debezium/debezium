CREATE TABLE simple (pk integer auto_increment, val integer not null, primary key(pk));

CREATE TABLE debezium_signal (
  id varchar(64),
  type varchar(32),
  data varchar(2048)
);
