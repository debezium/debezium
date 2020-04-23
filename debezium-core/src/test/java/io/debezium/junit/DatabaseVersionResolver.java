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

    public class DatabaseVersion {
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
                if (minor == -1 || dbVersionMinor < minor) {
                    return true;
                }
                else if (dbVersionMinor == minor) {
                    if (patch != -1 || dbVersionPatch < patch) {
                        return true;
                    }
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
                if (minor == -1 || dbVersionMinor > minor) {
                    return true;
                }
                else if (dbVersionMinor == minor) {
                    if (patch != -1 || dbVersionPatch > patch) {
                        return true;
                    }
                }
            }
            return false;
        }
    }
}
