/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.junit;

/**
 * Defines a contract on how to obtain the database version for a given source connector implementation.
 *
 * @author Chris Cranford
 */
public interface DatabaseVersionResolver {

    /**
     * Return the source database version.
     */
    DatabaseVersion getVersion();

    class DatabaseVersion {
        private final int dbVersionMajor;
        private final int dbVersionMinor;
        private final int dbVersionPatch;

        public DatabaseVersion(int major, int minor, int patch) {
            this.dbVersionMajor = major;
            this.dbVersionMinor = minor;
            this.dbVersionPatch = patch;
        }

        public int getMajor() {
            return dbVersionMajor;
        }

        public int getMinor() {
            return dbVersionMinor;
        }

        public int getPatch() {
            return dbVersionPatch;
        }

        public boolean isLessThan(int major, int minor, int patch) {
            if (dbVersionMajor < major) {
                return true;
            }
            else if (dbVersionMajor == major) {
                if (minor != -1 && dbVersionMinor < minor) {
                    // i.e. db 11.2 and annotation is 11.3
                    return true;
                }
                else if (dbVersionMinor == minor) {
                    // i.e. db 11.2.1 and annotation is 11.2.2
                    return patch != -1 && dbVersionPatch < patch;
                }
            }
            return false;
        }

        public boolean isLessThanEqualTo(int major, int minor, int patch) {
            return isLessThan(major, minor, patch) || isEqualTo(major, minor, patch);
        }

        public boolean isEqualTo(int major, int minor, int patch) {
            if (dbVersionMajor == major) {
                if (minor == -1 || dbVersionMinor == minor) {
                    if (patch == -1 || dbVersionPatch == patch) {
                        return true;
                    }
                }
            }
            return false;
        }

        public boolean isGreaterThanEqualTo(int major, int minor, int patch) {
            return isGreaterThan(major, minor, patch) || isEqualTo(major, minor, patch);
        }

        public boolean isGreaterThan(int major, int minor, int patch) {
            if (dbVersionMajor > major) {
                return true;
            }
            else if (dbVersionMajor == major) {
                if (minor == -1) {
                    if (dbVersionMinor == 0) {
                        if (patch == -1) {
                            if (dbVersionPatch == 0) {
                                // effectively i.e. 11.0.0 compared to 11, which isn't greater than.
                                // if this is needed, GreaterThanOrEqualTo should be used.
                                return false;
                            }
                            // i.e. 11.0.1 compared to 11
                            return dbVersionPatch > 0;
                        }
                        // i.e. 11.0.2 compared to 11.0.1
                        return dbVersionPatch > patch;
                    }
                    // i.e. 11.1 compared to 11
                    return dbVersionMinor > 0;
                }
                // i.e. 11.2 compared to 11.1
                return dbVersionMinor > minor;
            }
            return false;
        }

        @Override
        public String toString() {
            return dbVersionMajor + "." + dbVersionMinor + "." + dbVersionPatch;
        }
    }
}
