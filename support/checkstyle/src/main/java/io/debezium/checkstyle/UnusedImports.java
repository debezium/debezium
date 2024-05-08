/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.checkstyle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.puppycrawl.tools.checkstyle.api.DetailAST;
import com.puppycrawl.tools.checkstyle.api.FileContents;
import com.puppycrawl.tools.checkstyle.api.FullIdent;
import com.puppycrawl.tools.checkstyle.api.TextBlock;
import com.puppycrawl.tools.checkstyle.api.TokenTypes;
import com.puppycrawl.tools.checkstyle.checks.imports.UnusedImportsCheck;
import com.puppycrawl.tools.checkstyle.checks.javadoc.InvalidJavadocTag;
import com.puppycrawl.tools.checkstyle.checks.javadoc.JavadocTag;
import com.puppycrawl.tools.checkstyle.checks.javadoc.JavadocTags;
import com.puppycrawl.tools.checkstyle.utils.CommonUtil;
import com.puppycrawl.tools.checkstyle.utils.JavadocUtil.JavadocTagType;

/**
 * This is a specialization of the {@link UnusedImportsCheck} that fixes a couple of problems, including correctly processing
 * {@code @link} expressions that are not properly terminated on the same line, correctly processing JavaDoc {@code @param} lines,
 * and correctly processing method parameters contained with {@code @link} expressions.
 * <p>
 * Unfortunately, the base class is not easily overwritten, and thus a fair amount of the logic has to be incorporated here.
 * </p>
 */
public class UnusedImports extends UnusedImportsCheck {

    private static final String[] DEBUG_CLASSNAMES = {};
    private static final Set<String> DEBUG_CLASSNAMES_SET = new HashSet<>(Arrays.asList(DEBUG_CLASSNAMES));

    /**
     * A regular expression for finding the first word within a JavaDoc "@link" text.
     *
     * <pre>
     * (.*?)(?:\s+|#|\$)(.*)
     * </pre>
     */
    private static final Pattern LINK_VALUE_IN_TEXT_PATTERN = CommonUtil.createPattern("(.*?)(?:\\s+|#|\\$)(.*)");
    /**
     * A regular expression for finding the class name (group 1) and the method parameters (group 2) within a JavaDoc "@link"
     * reference.
     *
     * <pre>
     * ([\w.]+)(?:\#?\w+)?(?:\(([^\)]+)\))?.*
     * </pre>
     */
    private static final Pattern PARTS_OF_CLASS_OR_REFERENCE_PATTERN = CommonUtil.createPattern(
            "([\\w.]+)(?:\\#?\\w+)?(?:\\(([^\\)]+)\\))?.*");
    /**
     * A regular expression for finding the first classname referenced in a "@link" reference.
     *
     * <pre>
     * \{\@link\s+([^}]*)
     * </pre>
     */
    private static final Pattern LINK_VALUE_PATTERN = CommonUtil.createPattern("\\{\\@link\\s+([^}]*)");

    private boolean collect = false;
    private boolean processJavaDoc = false;
    private final Set<FullIdent> imports = new HashSet<>();
    private final Set<String> referenced = new HashSet<>();
    private boolean print = false;

    public UnusedImports() {
    }

    @Override
    public void setProcessJavadoc(boolean aValue) {
        super.setProcessJavadoc(aValue);
        processJavaDoc = aValue;
    }

    @Override
    public void beginTree(DetailAST aRootAST) {
        collect = false;
        imports.clear();
        referenced.clear();
        super.beginTree(aRootAST);
    }

    @Override
    public void visitToken(DetailAST aAST) {
        if (aAST.getType() == TokenTypes.CLASS_DEF) {
            String classname = aAST.findFirstToken(TokenTypes.IDENT).getText();
            print = DEBUG_CLASSNAMES_SET.contains(classname);
        }
        if (aAST.getType() == TokenTypes.IDENT) {
            super.visitToken(aAST);
            if (collect) {
                processIdent(aAST);
            }
        }
        else if (aAST.getType() == TokenTypes.IMPORT) {
            super.visitToken(aAST);
            processImport(aAST);
        }
        else if (aAST.getType() == TokenTypes.STATIC_IMPORT) {
            super.visitToken(aAST);
            processStaticImport(aAST);
        }
        else {
            collect = true;
            if (processJavaDoc) {
                processJavaDocLinkParameters(aAST);
                super.visitToken(aAST);
            }
        }
    }

    /**
     * Collects references made by IDENT.
     *
     * @param aAST the IDENT node to process {@link ArrayList stuff}
     */
    protected void processIdent(DetailAST aAST) {
        final int parentType = aAST.getParent().getType();
        if (((parentType != TokenTypes.DOT) && (parentType != TokenTypes.METHOD_DEF))
                || ((parentType == TokenTypes.DOT) && (aAST.getNextSibling() != null))) {
            referenced.add(aAST.getText());
        }
    }

    protected void processJavaDocLinkParameters(DetailAST aAST) {
        final FileContents contents = getFileContents();
        final int lineNo = aAST.getLineNo();
        final TextBlock cmt = contents.getJavadocBefore(lineNo);
        if (cmt != null) {
            final JavadocTags tags = JavaDocUtil.getJavadocTags(cmt, JavadocTagType.ALL);
            for (final JavadocTag tag : tags.getValidTags()) {
                processJavaDocTag(tag);
            }
            for (final InvalidJavadocTag tag : tags.getInvalidTags()) {
                log(tag.getLine(), tag.getCol(), "import.invalidJavaDocTag", tag.getName());
            }
        }
    }

    protected void processJavaDocTag(JavadocTag tag) {
        print("tag: ", tag);

        if (tag.canReferenceImports()) {
            String identifier = tag.getFirstArg();
            print("Found identifier: ", identifier);
            referenced.add(identifier);
            // Find the link to classes or methods ...
            final Matcher matcher = LINK_VALUE_IN_TEXT_PATTERN.matcher(identifier);
            while (matcher.find()) {
                // Capture the link ...
                identifier = matcher.group(1);
                referenced.add(identifier);
                print("Found new identifier: ", identifier);
                // Get the parameters ...
                String methodCall = matcher.group(2);
                processClassOrMethodReference(methodCall);
            }
        }
        else if (tag.isParamTag()) {
            String paramText = tag.getFirstArg();
            print("Found parameter text: ", paramText);
            // Find the links to classe
            Matcher paramsMatcher = LINK_VALUE_PATTERN.matcher(paramText);
            while (paramsMatcher.find()) {
                // Found a link ...
                String linkValue = paramsMatcher.group(1);
                processClassOrMethodReference(linkValue);
            }
        }
        else if (tag.isReturnTag()) {
            String returnText = tag.getFirstArg();
            print("Found return text: ", returnText);
            // Find the links to classe
            Matcher paramsMatcher = LINK_VALUE_PATTERN.matcher(returnText);
            while (paramsMatcher.find()) {
                // Found a link ...
                String linkValue = paramsMatcher.group(1);
                processClassOrMethodReference(linkValue);
            }
        }
    }

    protected void processClassOrMethodReference(String text) {
        print("Adding referenced: ", text);
        referenced.add(text);
        // Look for all the identifiers within the parameters ...
        Matcher paramsMatcher = PARTS_OF_CLASS_OR_REFERENCE_PATTERN.matcher(text);
        while (paramsMatcher.find()) {
            // This is a link; get the parameters ...
            String clazz = paramsMatcher.group(1);
            String params = paramsMatcher.group(2);
            if (clazz != null) {
                print("Found class: ", clazz);
                referenced.add(clazz);
            }
            if (params != null) {
                print("Found params: ", params);
                for (String param : params.split(",")) {
                    if ("...".equals(param)) {
                        continue;
                    }
                    param = param.replace("...", "").trim();
                    print("Found param: ", param);
                    referenced.add(param);
                }
            }
        }
    }

    /**
     * Collects the details of imports.
     *
     * @param aAST node containing the import details
     */
    private void processImport(DetailAST aAST) {
        final FullIdent name = FullIdent.createFullIdentBelow(aAST);
        if ((name != null) && !name.getText().endsWith(".*")) {
            imports.add(name);
        }
    }

    /**
     * Collects the details of static imports.
     *
     * @param aAST node containing the static import details
     */
    private void processStaticImport(DetailAST aAST) {
        final FullIdent name = FullIdent.createFullIdent(aAST.getFirstChild().getNextSibling());
        if ((name != null) && !name.getText().endsWith(".*")) {
            imports.add(name);
        }
    }

    @Override
    public void finishTree(DetailAST aRootAST) {
        // loop over all the imports to see if referenced.
        for (final FullIdent imp : imports) {
            if (!referenced.contains(CommonUtil.baseClassName(imp.getText()))) {
                print("imp.getText(): " + CommonUtil.baseClassName(imp.getText()));
                print("referenced: " + referenced);
                log(imp.getLineNo(), imp.getColumnNo(), "import.unused", imp.getText());
            }
        }
    }

    private void print(Object... messages) {
        if (print) {
            // CHECKSTYLE IGNORE check FOR NEXT 4 LINES
            for (Object msg : messages) {
                System.out.print(msg);
            }
            System.out.println();
        }
    }
}
