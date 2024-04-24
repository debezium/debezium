/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.errors.ConnectException;
import com.yugabyte.core.BaseConnection;
import com.yugabyte.core.TypeInfo;
import com.yugabyte.jdbc.PgDatabaseMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.annotation.Immutable;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.util.Collect;

/**
 * A registry of types supported by a PostgreSQL instance. Allows lookup of the types according to
 * type name or OID.
 *
 * @author Jiri Pechanec
 *
 */
public class TypeRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(TypeRegistry.class);

    public static final String TYPE_NAME_GEOGRAPHY = "geography";
    public static final String TYPE_NAME_GEOMETRY = "geometry";
    public static final String TYPE_NAME_CITEXT = "citext";
    public static final String TYPE_NAME_HSTORE = "hstore";
    public static final String TYPE_NAME_LTREE = "ltree";
    public static final String TYPE_NAME_ISBN = "isbn";

    public static final String TYPE_NAME_HSTORE_ARRAY = "_hstore";
    public static final String TYPE_NAME_GEOGRAPHY_ARRAY = "_geography";
    public static final String TYPE_NAME_GEOMETRY_ARRAY = "_geometry";
    public static final String TYPE_NAME_CITEXT_ARRAY = "_citext";
    public static final String TYPE_NAME_LTREE_ARRAY = "_ltree";

    public static final int NO_TYPE_MODIFIER = -1;
    public static final int UNKNOWN_LENGTH = -1;

    // PostgreSQL driver reports user-defined Domain types as Types.DISTINCT
    public static final int DOMAIN_TYPE = Types.DISTINCT;

    private static final String CATEGORY_ARRAY = "A";
    private static final String CATEGORY_ENUM = "E";

    private static final String SQL_ENUM_VALUES = "SELECT t.enumtypid as id, array_agg(t.enumlabel) as values "
            + "FROM pg_catalog.pg_enum t GROUP BY id";

    private static final String SQL_TYPES = "SELECT t.oid AS oid, t.typname AS name, t.typelem AS element, t.typbasetype AS parentoid, t.typtypmod as modifiers, t.typcategory as category, e.values as enum_values "
            + "FROM pg_catalog.pg_type t "
            + "JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid) "
            + "LEFT JOIN (" + SQL_ENUM_VALUES + ") e ON (t.oid = e.id) "
            + "WHERE n.nspname != 'pg_toast'";

    private static final String SQL_NAME_LOOKUP = SQL_TYPES + " AND t.typname = ?";

    private static final String SQL_OID_LOOKUP = SQL_TYPES + " AND t.oid = ?";

    private static final Map<String, String> LONG_TYPE_NAMES = Collections.unmodifiableMap(getLongTypeNames());

    private static Map<String, String> getLongTypeNames() {
        Map<String, String> longTypeNames = new HashMap<>();

        longTypeNames.put("bigint", "int8");
        longTypeNames.put("bit varying", "varbit");
        longTypeNames.put("boolean", "bool");
        longTypeNames.put("character", "bpchar");
        longTypeNames.put("character varying", "varchar");
        longTypeNames.put("double precision", "float8");
        longTypeNames.put("integer", "int4");
        longTypeNames.put("real", "float4");
        longTypeNames.put("smallint", "int2");
        longTypeNames.put("timestamp without time zone", "timestamp");
        longTypeNames.put("timestamp with time zone", "timestamptz");
        longTypeNames.put("time without time zone", "time");
        longTypeNames.put("time with time zone", "timetz");

        return longTypeNames;
    }

    private final Map<String, PostgresType> nameToType = new HashMap<>();
    private final Map<Integer, PostgresType> oidToType = new HashMap<>();

    private final PostgresConnection connection;
    private final SqlTypeMapper sqlTypeMapper;

    private int geometryOid = Integer.MIN_VALUE;
    private int geographyOid = Integer.MIN_VALUE;
    private int citextOid = Integer.MIN_VALUE;
    private int hstoreOid = Integer.MIN_VALUE;
    private int ltreeOid = Integer.MIN_VALUE;
    private int isbnOid = Integer.MIN_VALUE;

    private int hstoreArrayOid = Integer.MIN_VALUE;
    private int geometryArrayOid = Integer.MIN_VALUE;
    private int geographyArrayOid = Integer.MIN_VALUE;
    private int citextArrayOid = Integer.MIN_VALUE;
    private int ltreeArrayOid = Integer.MIN_VALUE;

    public TypeRegistry(PostgresConnection connection) {
        try {
            this.connection = connection;
            sqlTypeMapper = new SqlTypeMapper(this.connection);

            prime();
        }
        catch (SQLException e) {
            throw new DebeziumException("Couldn't initialize type registry", e);
        }
    }

    private void addType(PostgresType type) {
        oidToType.put(type.getOid(), type);
        nameToType.put(type.getName(), type);

        if (TYPE_NAME_GEOMETRY.equals(type.getName())) {
            geometryOid = type.getOid();
        }
        else if (TYPE_NAME_GEOGRAPHY.equals(type.getName())) {
            geographyOid = type.getOid();
        }
        else if (TYPE_NAME_CITEXT.equals(type.getName())) {
            citextOid = type.getOid();
        }
        else if (TYPE_NAME_HSTORE.equals(type.getName())) {
            hstoreOid = type.getOid();
        }
        else if (TYPE_NAME_LTREE.equals(type.getName())) {
            ltreeOid = type.getOid();
        }
        else if (TYPE_NAME_HSTORE_ARRAY.equals(type.getName())) {
            hstoreArrayOid = type.getOid();
        }
        else if (TYPE_NAME_GEOMETRY_ARRAY.equals(type.getName())) {
            geometryArrayOid = type.getOid();
        }
        else if (TYPE_NAME_GEOGRAPHY_ARRAY.equals(type.getName())) {
            geographyArrayOid = type.getOid();
        }
        else if (TYPE_NAME_CITEXT_ARRAY.equals(type.getName())) {
            citextArrayOid = type.getOid();
        }
        else if (TYPE_NAME_LTREE_ARRAY.equals(type.getName())) {
            ltreeArrayOid = type.getOid();
        }
        else if (TYPE_NAME_ISBN.equals(type.getName())) {
            isbnOid = type.getOid();
        }
    }

    /**
     *
     * @param oid - PostgreSQL OID
     * @return type associated with the given OID
     */
    public PostgresType get(int oid) {
        PostgresType r = oidToType.get(oid);
        if (r == null) {
            r = resolveUnknownType(oid);
            if (r == null) {
                LOGGER.warn("Unknown OID {} requested", oid);
                r = PostgresType.UNKNOWN;
            }
        }
        return r;
    }

    /**
     *
     * @param name - PostgreSQL type name
     * @return type associated with the given type name
     */
    public PostgresType get(String name) {
        switch (name) {
            case "serial":
                name = "int4";
                break;
            case "smallserial":
                name = "int2";
                break;
            case "bigserial":
                name = "int8";
                break;
        }
        String[] parts = name.split("\\.");
        if (parts.length > 1) {
            name = parts[1];
        }
        if (name.charAt(0) == '"') {
            name = name.substring(1, name.length() - 1);
        }
        PostgresType r = nameToType.get(name);
        if (r == null) {
            r = resolveUnknownType(name);
            if (r == null) {
                LOGGER.warn("Unknown type named {} requested", name);
                r = PostgresType.UNKNOWN;
            }
        }
        return r;
    }

    public Map<String, PostgresType> getRegisteredTypes() {
        return Collections.unmodifiableMap(nameToType);
    }

    /**
     *
     * @return OID for {@code GEOMETRY} type of this PostgreSQL instance
     */
    public int geometryOid() {
        return geometryOid;
    }

    /**
     *
     * @return OID for {@code GEOGRAPHY} type of this PostgreSQL instance
     */
    public int geographyOid() {
        return geographyOid;
    }

    /**
     *
     * @return OID for {@code CITEXT} type of this PostgreSQL instance
     */
    public int citextOid() {
        return citextOid;
    }

    /**
     *
     * @return OID for {@code HSTORE} type of this PostgreSQL instance
     */
    public int hstoreOid() {
        return hstoreOid;
    }

    /**
     *
     * @return OID for {@code LTREE} type of this PostgreSQL instance
     */
    public int ltreeOid() {
        return ltreeOid;
    }

    /**
     *
     * @return OID for {@code ISBN} type of this PostgreSQL instance
     */
    public int isbnOid() {
        return isbnOid;
    }

    /**
    *
    * @return OID for array of {@code HSTORE} type of this PostgreSQL instance
    */
    public int hstoreArrayOid() {
        return hstoreArrayOid;
    }

    /**
     *
     * @return OID for array of {@code GEOMETRY} type of this PostgreSQL instance
     */
    public int geometryArrayOid() {
        return geometryArrayOid;
    }

    /**
     *
     * @return OID for array of {@code GEOGRAPHY} type of this PostgreSQL instance
     */
    public int geographyArrayOid() {
        return geographyArrayOid;
    }

    /**
     *
     * @return OID for array of {@code CITEXT} type of this PostgreSQL instance
     */
    public int citextArrayOid() {
        return citextArrayOid;
    }

    /**
     *
     * @return OID for array of {@code LTREE} type of this PostgreSQL instance
     */
    public int ltreeArrayOid() {
        return ltreeArrayOid;
    }

    /**
     * Converts a type name in long (readable) format like <code>boolean</code> to s standard
     * data type name like <code>bool</code>.
     *
     * @param typeName - a type name in long format
     * @return - the type name in standardized format
     */
    public static String normalizeTypeName(String typeName) {
        return LONG_TYPE_NAMES.getOrDefault(typeName, typeName);
    }

    /**
     * Prime the {@link TypeRegistry} with all existing database types
     */
    private void prime() throws SQLException {
        try (Statement statement = connection.connection().createStatement();
                ResultSet rs = statement.executeQuery(SQL_TYPES)) {
            final List<PostgresType.Builder> delayResolvedBuilders = new ArrayList<>();
            while (rs.next()) {
                PostgresType.Builder builder = createTypeBuilderFromResultSet(rs);

                // If the type does have a base type, we can build/add immediately.
                if (!builder.hasParentType()) {
                    addType(builder.build());
                    continue;
                }

                // For types with base type mappings, they need to be delayed.
                delayResolvedBuilders.add(builder);
            }

            // Resolve delayed builders
            for (PostgresType.Builder builder : delayResolvedBuilders) {
                addType(builder.build());
            }
        }
    }

    private PostgresType.Builder createTypeBuilderFromResultSet(ResultSet rs) throws SQLException {
        // Coerce long to int so large unsigned values are represented as signed
        // Same technique is used in TypeInfoCache
        final int oid = (int) rs.getLong("oid");
        final int parentTypeOid = (int) rs.getLong("parentoid");
        final int modifiers = (int) rs.getLong("modifiers");
        String typeName = rs.getString("name");
        String category = rs.getString("category");

        PostgresType.Builder builder = new PostgresType.Builder(
                this,
                typeName,
                oid,
                sqlTypeMapper.getSqlType(typeName),
                modifiers,
                getTypeInfo(connection));

        if (CATEGORY_ENUM.equals(category)) {
            String[] enumValues = (String[]) rs.getArray("enum_values").getArray();
            builder = builder.enumValues(Arrays.asList(enumValues));
        }
        else if (CATEGORY_ARRAY.equals(category)) {
            builder = builder.elementType((int) rs.getLong("element"));
        }
        return builder.parentType(parentTypeOid);
    }

    private PostgresType resolveUnknownType(String name) {
        try {
            LOGGER.trace("Type '{}' not cached, attempting to lookup from database.", name);

            try (PreparedStatement statement = connection.connection().prepareStatement(SQL_NAME_LOOKUP)) {
                statement.setString(1, name);
                return loadType(statement);
            }
        }
        catch (SQLException e) {
            throw new ConnectException("Database connection failed during resolving unknown type", e);
        }
    }

    private PostgresType resolveUnknownType(int lookupOid) {
        try {
            LOGGER.trace("Type OID '{}' not cached, attempting to lookup from database.", lookupOid);

            try (PreparedStatement statement = connection.connection().prepareStatement(SQL_OID_LOOKUP)) {
                statement.setInt(1, lookupOid);
                return loadType(statement);
            }
        }
        catch (SQLException e) {
            throw new ConnectException("Database connection failed during resolving unknown type", e);
        }
    }

    private PostgresType loadType(PreparedStatement statement) throws SQLException {
        try (ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                PostgresType result = createTypeBuilderFromResultSet(rs).build();
                addType(result);
                return result;
            }
        }
        return null;
    }

    public int isbn() {
        return isbnOid;
    }

    /**
     * Allows to obtain the SQL type corresponding to PG types. This uses a custom statement instead of going through
     * {@link PgDatabaseMetaData#getTypeInfo()} as the latter causes N+1 SELECTs, making it very slow on installations
     * with many custom types.
     *
     * @author Gunnar Morling
     * @see DBZ-899
     */
    private static class SqlTypeMapper {

        /**
         * Based on com.yugabyte.jdbc.TypeInfoCache.getSQLType(String). To emulate the original statement's behavior
         * (which works for single types only), PG's DISTINCT ON extension is used to just return the first entry should a
         * type exist in multiple schemas.
         */
        private static final String SQL_TYPE_DETAILS = "SELECT DISTINCT ON (typname) typname, typinput='array_in'::regproc, typtype, sp.r, pg_type.oid "
                + "  FROM pg_catalog.pg_type "
                + "  LEFT "
                + "  JOIN (select ns.oid as nspoid, ns.nspname, r.r "
                + "          from pg_namespace as ns "
                // -- go with older way of unnesting array to be compatible with 8.0
                + "          join ( select s.r, (current_schemas(false))[s.r] as nspname "
                + "                   from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r "
                + "         using ( nspname ) "
                + "       ) as sp "
                + "    ON sp.nspoid = typnamespace "
                + " ORDER BY typname, sp.r, pg_type.oid;";

        private final PostgresConnection connection;

        @Immutable
        private final Set<String> preloadedSqlTypes;

        @Immutable
        private final Map<String, Integer> sqlTypesByPgTypeNames;

        private SqlTypeMapper(PostgresConnection connection) throws SQLException {
            this.connection = connection;
            this.preloadedSqlTypes = Collect.unmodifiableSet(getTypeInfo(connection).getPGTypeNamesWithSQLTypes());
            this.sqlTypesByPgTypeNames = Collections.unmodifiableMap(getSqlTypes(connection));
        }

        public int getSqlType(String typeName) throws SQLException {
            boolean isCoreType = preloadedSqlTypes.contains(typeName);

            // obtain core types such as bool, int2 etc. from the driver, as it correctly maps these types to the JDBC
            // type codes. Also those values are cached in TypeInfoCache.
            if (isCoreType) {
                return getTypeInfo(connection).getSQLType(typeName);
            }
            if (typeName.endsWith("[]")) {
                return Types.ARRAY;
            }
            // get custom type mappings from the map which was built up with a single query
            else {
                try {
                    final Integer pgType = sqlTypesByPgTypeNames.get(typeName);
                    if (pgType != null) {
                        return pgType;
                    }
                    LOGGER.info("Failed to obtain SQL type information for type {} via custom statement, falling back to TypeInfo#getSQLType()", typeName);
                    return getTypeInfo(connection).getSQLType(typeName);
                }
                catch (Exception e) {
                    LOGGER.warn("Failed to obtain SQL type information for type {} via custom statement, falling back to TypeInfo#getSQLType()", typeName, e);
                    return getTypeInfo(connection).getSQLType(typeName);
                }
            }
        }

        /**
         * Builds up a map of SQL (JDBC) types by PG type name; contains only values for non-core types.
         */
        private static Map<String, Integer> getSqlTypes(PostgresConnection connection) throws SQLException {
            Map<String, Integer> sqlTypesByPgTypeNames = new HashMap<>();

            try (Statement statement = connection.connection().createStatement()) {
                try (ResultSet rs = statement.executeQuery(SQL_TYPE_DETAILS)) {
                    while (rs.next()) {
                        int type;
                        boolean isArray = rs.getBoolean(2);
                        String typtype = rs.getString(3);
                        if (isArray) {
                            type = Types.ARRAY;
                        }
                        else if ("c".equals(typtype)) {
                            type = Types.STRUCT;
                        }
                        else if ("d".equals(typtype)) {
                            type = Types.DISTINCT;
                        }
                        else if ("e".equals(typtype)) {
                            type = Types.VARCHAR;
                        }
                        else {
                            type = Types.OTHER;
                        }

                        sqlTypesByPgTypeNames.put(rs.getString(1), type);
                    }
                }
            }

            return sqlTypesByPgTypeNames;
        }
    }

    private static TypeInfo getTypeInfo(PostgresConnection connection) throws SQLException {
        return ((BaseConnection) connection.connection()).getTypeInfo();
    }
}
