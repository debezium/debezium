/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.checkstyle;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import com.puppycrawl.tools.checkstyle.api.FileText;

public class Header extends com.puppycrawl.tools.checkstyle.checks.header.HeaderCheck {

    private Set<String> excludedFileSet;
    private String excludedFilesRegex;
    private Pattern excludedFilesPattern;
    private final String workingDirPath = new File(".").getAbsoluteFile().getParentFile().getAbsolutePath();
    private final int workingDirPathLength = workingDirPath.length();

    public Header() {
    }

    public void setExcludedFilesRegex(String excludedFilePattern) {
        this.excludedFilesRegex = excludedFilePattern;
        this.excludedFilesPattern = Pattern.compile(this.excludedFilesRegex);
    }

    public void setExcludedClasses(String excludedClasses) {
        this.excludedFileSet = new HashSet<>();
        if (excludedClasses != null) {
            for (String classname : excludedClasses.split(",")) {
                if (classname != null && classname.trim().length() != 0) {
                    String path = classname.trim().replace('.', '/') + ".java"; // change package names to filenames ...
                    this.excludedFileSet.add(path.trim());
                }
            }
        }
    }

    public void setHeaderFile(String aFileName) {
        // Load the file from the file ...
        InputStream stream = this.getClass().getClassLoader().getResourceAsStream("debezium.header");
        if (stream == null) {
            throw new RuntimeException("unable to load header file (using classloader) " + aFileName);
        }
        // Load the contents and place into the lines ...
        try {
            final LineNumberReader lnr = new LineNumberReader(new InputStreamReader(stream));
            StringBuilder sb = new StringBuilder();
            while (true) {
                final String l = lnr.readLine();
                if (l == null) {
                    break;
                }
                sb.append(l).append("\\n");
            }
            super.setHeader(sb.toString());
        } catch (IOException e) {
            throw new RuntimeException("problem reading header file (using classloader) " + aFileName, e);
        }
    }

    protected boolean isExcluded(File file) {
        // See whether this file is excluded ...
        String filename = file.getAbsolutePath().replace(File.separator, "/");
        if (filename.startsWith(workingDirPath)) {
            filename = filename.substring(workingDirPathLength);
        }
        filename = filename.replaceAll(".*/src/(main|test)/(java|resources)/", "");

        // First try one of the explicit class names ...
        for (String excludedFileName : excludedFileSet) {
            if (filename.endsWith(excludedFileName)) {
                return true;
            }
        }

        // Next try to evaluate the pattern ...
        if (excludedFilesPattern != null && excludedFilesPattern.matcher(filename).matches()) {
            return true;
        }
        return false;
    }

    @Override
    protected void processFiltered(File aFile, FileText fileText) {
        if (isExcluded(aFile)) {
            return;
        }
        super.processFiltered(aFile, fileText);
    }
}
