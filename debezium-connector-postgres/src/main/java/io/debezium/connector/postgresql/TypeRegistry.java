/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public static final String TYPE_NAME_HSTORE_ARRAY = "_hstore";
    public static final String TYPE_NAME_GEOGRAPHY_ARRAY = "_geography";
    public static final String TYPE_NAME_GEOMETRY_ARRAY = "_geometry";
    public static final String TYPE_NAME_CITEXT_ARRAY = "_citext";
    public static final String TYPE_NAME_LTREE_ARRAY = "_ltree";

    public static final int NO_TYPE_MODIFIER = -1;
    public static final int UNKNOWN_LENGTH = -1;

    // PostgreSQL driver reports user-defined Domain types as Types.DISTINCT
    public static final int DOMAIN_TYPE = Types.DISTINCT;

    private static final String CATEGORY_ENUM = "E";

    private static final String SQL_NON_ARRAY_TYPES = "SELECT t.oid AS oid, t.typname AS name, t.typbasetype AS parentoid, t.typtypmod as modifiers, t.typcategory as category "
            + "FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid) "
            + "WHERE n.nspname != 'pg_toast' AND t.typcategory <> 'A'";

    private static final String SQL_ARRAY_TYPES = "SELECT t.oid AS oid, t.typname AS name, t.typelem AS element, t.typbasetype AS parentoid, t.typtypmod as modifiers, t.typcategory as category "
            + "FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid) "
            + "WHERE n.nspname != 'pg_toast' AND t.typcategory = 'A'";

    private static final String SQL_NON_ARRAY_TYPE_NAME_LOOKUP = "SELECT t.oid as oid, t.typname AS name, t.typbasetype AS parentoid, t.typtypmod AS modifiers, t.typcategory as category "
            + "FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid) "
            + "WHERE n.nspname != 'pg_toast' AND t.typcategory <> 'A' AND t.typname = ?";

    private static final String SQL_NON_ARRAY_TYPE_OID_LOOKUP = "SELECT t.oid as oid, t.typname AS name, t.typbasetype AS parentoid, t.typtypmod AS modifiers, t.typcategory as category "
            + "FROM pg_catalog.pg_type t JOIN pg_catalog.pg_namespace n ON (t.typnamespace = n.oid) "
            + "WHERE n.nspname != 'pg_toast' AND t.typcategory <> 'A' AND t.oid = ?";

    private static final String SQL_ENUM_VALUES_LOOKUP = "select t.enumlabel as enum_value "
            + "FROM pg_catalog.pg_enum t "
            + "WHERE t.enumtypid=? ORDER BY t.enumsortorder";

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

    private int geometryOid = Integer.MIN_VALUE;
    private int geographyOid = Integer.MIN_VALUE;
    private int citextOid = Integer.MIN_VALUE;
    private int hstoreOid = Integer.MIN_VALUE;
    private int ltreeOid = Integer.MIN_VALUE;

    private int hstoreArrayOid = Integer.MIN_VALUE;
    private int geometryArrayOid = Integer.MIN_VALUE;
    private int geographyArrayOid = Integer.MIN_VALUE;
    private int citextArrayOid = Integer.MIN_VALUE;
    private int ltreeArrayOid = Integer.MIN_VALUE;

    public TypeRegistry(PostgresConnection connection) {
        this.connection = connection;
        prime();
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
    private void prime() {
        Connection pgConnection = null;
        try {
            pgConnection = connection.connection();

            final TypeInfo typeInfo = ((BaseConnection) pgConnection).getTypeInfo();
            final SqlTypeMapper sqlTypeMapper = new SqlTypeMapper(pgConnection, typeInfo);

            try (final Statement statement = pgConnection.createStatement()) {
                // Read non-array types
                try (final ResultSet rs = statement.executeQuery(SQL_NON_ARRAY_TYPES)) {
                    final List<PostgresType.Builder> delayResolvedBuilders = new ArrayList<>();
                    while (rs.next()) {
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
                                typeInfo);

                        if (CATEGORY_ENUM.equals(category)) {
                            builder = builder.enumValues(resolveEnumValues(pgConnection, oid));
                        }

                        // If the type does have have a base type, we can build/add immediately.
                        if (parentTypeOid == 0) {
                            addType(builder.build());
                            continue;
                        }

                        // For types with base type mappings, they need to be delayed.
                        builder = builder.parentType(parentTypeOid);
                        delayResolvedBuilders.add(builder);
                    }

                    // Resolve delayed builders
                    for (PostgresType.Builder builder : delayResolvedBuilders) {
                        addType(builder.build());
                    }
                }

                // Read array types
                try (final ResultSet rs = statement.executeQuery(SQL_ARRAY_TYPES)) {
                    final List<PostgresType.Builder> delayResolvedBuilders = new ArrayList<>();
                    while (rs.next()) {
                        // int2vector and oidvector will not be treated as arrays
                        final int oid = (int) rs.getLong("oid");
                        final int parentTypeOid = (int) rs.getLong("parentoid");
                        final int modifiers = (int) rs.getLong("modifiers");
                        String typeName = rs.getString("name");

                        PostgresType.Builder builder = new PostgresType.Builder(
                                this,
                                typeName,
                                oid,
                                sqlTypeMapper.getSqlType(typeName),
                                modifiers,
                                typeInfo);

                        builder = builder.elementType((int) rs.getLong("element"));

                        // If the type doesnot have a base type, we can build/add immediately
                        if (parentTypeOid == 0) {
                            addType(builder.build());
                            continue;
                        }

                        // For types with base type mappings, they need to be delayed.
                        builder = builder.parentType(parentTypeOid);
                        delayResolvedBuilders.add(builder);
                    }

                    // Resolve delayed builders
                    for (PostgresType.Builder builder : delayResolvedBuilders) {
                        addType(builder.build());
                    }
                }
            }

        }
        catch (SQLException e) {
            if (pgConnection == null) {
                throw new ConnectException("Could not create PG connection", e);
            }
            else {
                throw new ConnectException("Could not initialize type registry", e);
            }
        }
    }

    private PostgresType resolveUnknownType(String name) {
        try {
            LOGGER.trace("Type '{}' not cached, attempting to lookup from database.", name);
            final Connection connection = this.connection.connection();
            final TypeInfo typeInfo = ((BaseConnection) connection).getTypeInfo();
            final SqlTypeMapper sqlTypeMapper = new SqlTypeMapper(connection, typeInfo);

            try (final PreparedStatement statement = connection.prepareStatement(SQL_NON_ARRAY_TYPE_NAME_LOOKUP)) {
                statement.setString(1, name);
                try (final ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
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
                                typeInfo);

                        if (CATEGORY_ENUM.equals(category)) {
                            builder = builder.enumValues(resolveEnumValues(connection, oid));
                        }

                        PostgresType result = builder.parentType(parentTypeOid).build();
                        addType(result);

                        return result;
                    }
                }
            }
        }
        catch (SQLException e) {
            throw new ConnectException("Database connection failed during resolving unknown type", e);
        }

        return null;
    }

    private PostgresType resolveUnknownType(int lookupOid) {
        try {
            LOGGER.trace("Type OID '{}' not cached, attempting to lookup from database.", lookupOid);
            final Connection connection = this.connection.connection();
            final TypeInfo typeInfo = ((BaseConnection) connection).getTypeInfo();
            final SqlTypeMapper sqlTypeMapper = new SqlTypeMapper(connection, typeInfo);

            try (final PreparedStatement statement = connection.prepareStatement(SQL_NON_ARRAY_TYPE_OID_LOOKUP)) {
                statement.setInt(1, lookupOid);
                try (final ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
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
                                typeInfo);

                        if (CATEGORY_ENUM.equals(category)) {
                            builder = builder.enumValues(resolveEnumValues(connection, oid));
                        }

                        PostgresType result = builder.parentType(parentTypeOid).build();
                        addType(result);

                        return result;
                    }
                }
            }
        }
        catch (SQLException e) {
            throw new ConnectException("Database connection failed during resolving unknown type", e);
        }

        return null;
    }

    private List<String> resolveEnumValues(Connection pgConnection, int enumOid) throws SQLException {
        List<String> enumValues = new ArrayList<>();
        try (final PreparedStatement enumStatement = pgConnection.prepareStatement(SQL_ENUM_VALUES_LOOKUP)) {
            enumStatement.setInt(1, enumOid);
            try (final ResultSet enumRs = enumStatement.executeQuery()) {
                while (enumRs.next()) {
                    enumValues.add(enumRs.getString("enum_value"));
                }
            }
        }
        return enumValues.isEmpty() ? null : enumValues;
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
         * Based on org.postgresql.jdbc.TypeInfoCache.getSQLType(String). To emulate the original statement's behavior
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

        private final TypeInfo typeInfo;
        private final Set<String> preloadedSqlTypes;
        private final Map<String, Integer> sqlTypesByPgTypeNames;

        private SqlTypeMapper(Connection db, TypeInfo typeInfo) throws SQLException {
            this.typeInfo = typeInfo;
            this.preloadedSqlTypes = Collect.unmodifiableSet(typeInfo.getPGTypeNamesWithSQLTypes());
            this.sqlTypesByPgTypeNames = getSqlTypes(db, typeInfo);
        }

        public int getSqlType(String typeName) throws SQLException {
            boolean isCoreType = preloadedSqlTypes.contains(typeName);

            // obtain core types such as bool, int2 etc. from the driver, as it correctly maps these types to the JDBC
            // type codes. Also those values are cached in TypeInfoCache.
            if (isCoreType) {
                return typeInfo.getSQLType(typeName);
            }
            if (typeName.endsWith("[]")) {
                return Types.ARRAY;
            }
            // get custom type mappings from the map which was built up with a single query
            else {
                try {
                    return sqlTypesByPgTypeNames.get(typeName);
                }
                catch (Exception e) {
                    LOGGER.warn("Failed to obtain SQL type information for type {} via custom statement, falling back to TypeInfo#getSQLType()", typeName, e);
                    return typeInfo.getSQLType(typeName);
                }
            }
        }

        /**
         * Builds up a map of SQL (JDBC) types by PG type name; contains only values for non-core types.
         */
        private static Map<String, Integer> getSqlTypes(Connection db, TypeInfo typeInfo) throws SQLException {
            Map<String, Integer> sqlTypesByPgTypeNames = new HashMap<>();

            try (final Statement statement = db.createStatement()) {
                try (final ResultSet rs = statement.executeQuery(SQL_TYPE_DETAILS)) {
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
}
