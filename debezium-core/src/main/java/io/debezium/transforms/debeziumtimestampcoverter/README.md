SMT - Debezium Timestamp Converter:(Extended Version of SMT timestampConverter from connect, supporting debezium or others).
Also works for any kind of nested org.apache.kafka.connect.data.Struct data.

1. Connect config's:
	Would split the usage into two parts,

	a.Straight fields, like ts_ms in debezium.

		field – the field name (optional, can be left out in case of primitive data)
		target.type – desired type (i.e. string, long, Date, Time, Timestamp)
		format – in case converting to or from a string, a SimpleDateFormat-compatible format string

		example:
		"transforms":"TimestampConv"
		"transforms.TimestampConv.type":"org.telmate.SMT.TimestampConverter$Value"
		"transforms.TimestampConv.field":"ts_ms"
		"transforms.TimestampConvtarget.type":"Timestamp"
		"transforms.TimestampConv.timestamp.format":"yyyy-MM-dd hh:mm:ss"
		"transforms.TimestampConv.date.format":"yyyy-MM-dd"


	b.Nested fields, like after or before.

		struct.field - the field which is of Struct type(has nested data)
		field.type - is a comma seperated string, providing an option to add multiple converters.
					<Type of Timestamp Object> -> <To Target type>
					can add multiple type converters, <Type of Timestamp Object> -> <To Target type>,<Type of Timestamp Object> -> <To Target type>


		example:
		"transforms.TimestampConv.struct.field":"after"
		"transforms.TimestampConv.field.type":"io.debezium.time.Timestamp->string,io.debezium.time.Date->string"

	c. Can use the combination of both (a) and (b).
	

Default Timestamp/Date Format
==============================
Timestamp: yyyy-MM-dd HH:mm:ss
Date: yyyy-MM-dd
Can be overrided using the below two properties:
{timestamp.format and date.format}
	
Few observations from debezium data
=================================
Debezium message data contains all fields names and types in the schema, but payload would only contain the value for the fields which are not null.Inorder to avoid all the validate checks that connect imposes during transformation, i have used optional fields, which by passes the checks and connects understands that the field is not required to have a value.


Example:
=======
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" localhost:8083/connectors/ -d '{ "name": "deb_stagingQA", "config": { "connector.class": "io.debezium.connector.mysql.MySqlConnector", "tasks.max": "1","database.hostname": "**********", "database.port": "3306", "database.user": "*******", "database.password": "*******", "database.server.id": "*****", "database.server.name": "********","database.history.kafka.bootstrap.servers": "localhost:9092", "database.history.kafka.topic": "DBQA.history","table.whitelist":"********","transforms":"TimestampConv","transforms.TimestampConv.type":"org.telmate.SMT.DebeziumTimestampConverter$Value","transforms.TimestampConv.field":"ts_ms","transforms.TimestampConv.target.type":"Timestamp","transforms.TimestampConv.field.type":"io.debezium.time.Timestamp->string,io.debezium.time.Date->string","transforms.TimestampConv.struct.field":"after,before","transforms.TimestampConv.timestamp.format":"yyyy-MM-dd hh:mm:ss","snapshot.mode":"schema_only" } }'