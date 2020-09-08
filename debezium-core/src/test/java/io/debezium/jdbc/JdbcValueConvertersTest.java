package io.debezium.jdbc;

import io.debezium.relational.Column;
import junit.framework.TestCase;
import org.apache.kafka.connect.data.Field;

public class JdbcValueConvertersTest extends TestCase {

    private class JdbcVCTestImpl extends JdbcValueConverters {
        public Object proxy_convertFloat(Column clm, Field field, Object data) {
            return super.convertFloat(clm, field, data);
        }
    }

    public void testConvertFloat() {
        // I expect execution path to not hit column nor field
        assertEquals(9.2424642f, new JdbcVCTestImpl().proxy_convertFloat(null, null, 9.2424642f));
    }
}