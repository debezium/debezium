/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.checkstyle;

import java.util.HashSet;

import com.puppycrawl.tools.checkstyle.api.AbstractCheck;
import com.puppycrawl.tools.checkstyle.api.DetailAST;
import com.puppycrawl.tools.checkstyle.api.FullIdent;
import com.puppycrawl.tools.checkstyle.api.TokenTypes;
import com.puppycrawl.tools.checkstyle.utils.CommonUtil;

/**
 * A simple CheckStyle checker to verify specific import statements are not being used.
 *
 * @author Sanne Grinovero
 */
public class IllegalImport extends AbstractCheck {

    private final HashSet<String> notAllowedImports = new HashSet<>();
    private String message = "";

    /**
     * Set the list of illegal import statements.
     *
     * @param importStatements array of illegal packages
     */
    public void setIllegalClassnames(String[] importStatements) {
        for (String impo : importStatements) {
            notAllowedImports.add(impo);
        }
    }

    public void setMessage(String message) {
        if (message != null) {
            this.message = message;
        }
    }

    @Override
    public int[] getDefaultTokens() {
        return new int[]{ TokenTypes.IMPORT, TokenTypes.STATIC_IMPORT };
    }

    @Override
    public int[] getAcceptableTokens() {
        final int[] defaultTokens = getDefaultTokens();
        final int[] copy = new int[defaultTokens.length];
        System.arraycopy(defaultTokens, 0, copy, 0, defaultTokens.length);
        return copy;
    }

    @Override
    public int[] getRequiredTokens() {
        return CommonUtil.EMPTY_INT_ARRAY;
    }

    @Override
    public void visitToken(DetailAST aAST) {
        final FullIdent imp;
        if (aAST.getType() == TokenTypes.IMPORT) {
            imp = FullIdent.createFullIdentBelow(aAST);
        }
        else {
            // handle case of static imports of method names
            imp = FullIdent.createFullIdent(aAST.getFirstChild().getNextSibling());
        }
        final String text = imp.getText();
        if (isIllegalImport(text)) {
            final String message = buildError(text);
            log(aAST.getLineNo(), aAST.getColumnNo(), message, text);
        }
    }

    private String buildError(String importStatement) {
        return "Import statement violating a checkstyle rule: " + importStatement + ". " + message;
    }

    private boolean isIllegalImport(String importString) {
        return notAllowedImports.contains(importString);
    }
}
