package io.debezium.checkstyle;


import com.puppycrawl.tools.checkstyle.api.DetailAST;
import com.puppycrawl.tools.checkstyle.api.TokenTypes;
import com.puppycrawl.tools.checkstyle.checks.blocks.NeedBracesCheck;

public class NeedBraces extends NeedBracesCheck {

    public static final String MSG_KEY_NEED_BRACES = "Need Braces";


    @Override
    public int[] getDefaultTokens() {
        return new int[]{TokenTypes.LITERAL_IF, TokenTypes.LITERAL_ELSE};
    }


    @Override
    public int[] getAcceptableTokens() {
        return new int[]{TokenTypes.LITERAL_IF, TokenTypes.LITERAL_ELSE};
    }

    @Override
    public void visitToken(DetailAST ast) {
        final DetailAST slistAST = ast.findFirstToken(TokenTypes.SLIST);
        boolean isElseIf = false;
        boolean isIfElse = false;
        if (ast.getType() == TokenTypes.LITERAL_ELSE) {
            isElseIf = true;
        } else {
            if (ast.getType() == TokenTypes.LITERAL_IF) {
                isIfElse = true;
            }
        }

        final boolean isInAnnotationField = isInAnnotationField(ast);
        if (slistAST == null && !isElseIf && !isIfElse && !isInAnnotationField) {
            this.log(ast.getLineNo(), MSG_KEY_NEED_BRACES, ast.getText());
        }
    }

    private static boolean isInAnnotationField(DetailAST ast) {
        boolean isDefaultInAnnotation = false;
        if (ast.getParent().getType() == TokenTypes.ANNOTATION_FIELD_DEF) {
            isDefaultInAnnotation = true;
        }
        return isDefaultInAnnotation;
    }


}
