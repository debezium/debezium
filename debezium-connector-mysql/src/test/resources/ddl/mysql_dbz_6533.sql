CREATE TABLE tablename_suffix (
  PRIMARY KEY (id),
  id INTEGER NOT NULL AUTO_INCREMENT
);

CREATE TABLE tablename (
  PRIMARY KEY (id),
  id INTEGER NOT NULL AUTO_INCREMENT
);

CREATE TABLE another (
  PRIMARY KEY (id),
  id INTEGER NOT NULL AUTO_INCREMENT
);

INSERT INTO tablename_suffix VALUES (default);
INSERT INTO tablename VALUES (default);
INSERT INTO another VALUES (default);