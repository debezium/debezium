/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.checkstyle;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import com.puppycrawl.tools.checkstyle.api.TextBlock;
import com.puppycrawl.tools.checkstyle.checks.javadoc.InvalidJavadocTag;
import com.puppycrawl.tools.checkstyle.checks.javadoc.JavadocTag;
import com.puppycrawl.tools.checkstyle.checks.javadoc.JavadocTagInfo;
import com.puppycrawl.tools.checkstyle.checks.javadoc.JavadocTags;
import com.puppycrawl.tools.checkstyle.utils.CommonUtil;
import com.puppycrawl.tools.checkstyle.utils.JavadocUtil.JavadocTagType;

public final class JavaDocUtil {

    private JavaDocUtil() {
    }

    /**
     * Gets validTags from a given piece of Javadoc.
     *
     * @param aCmt the Javadoc comment to process.
     * @param aTagType the type of validTags we're interested in
     * @return all standalone validTags from the given javadoc.
     */
    public static JavadocTags getJavadocTags(TextBlock aCmt, JavadocTagType aTagType) {
        final String[] text = aCmt.getText();
        final List<JavadocTag> tags = Lists.newArrayList();
        final List<InvalidJavadocTag> invalidTags = Lists.newArrayList();
        Pattern blockTagPattern = CommonUtil.createPattern("/\\*{2,}\\s*@(\\p{Alpha}+)\\s");
        for (int i = 0; i < text.length; i++) {
            final String s = text[i];
            final Matcher blockTagMatcher = blockTagPattern.matcher(s);
            if ((aTagType.equals(JavadocTagType.ALL) || aTagType.equals(JavadocTagType.BLOCK)) && blockTagMatcher.find()) {
                final String tagName = blockTagMatcher.group(1);
                String content = s.substring(blockTagMatcher.end(1));
                if (content.endsWith("*/")) {
                    content = content.substring(0, content.length() - 2);
                }
                final int line = aCmt.getStartLineNo() + i;
                int col = blockTagMatcher.start(1) - 1;
                if (i == 0) {
                    col += aCmt.getStartColNo();
                }
                if (JavadocTagInfo.isValidName(tagName)) {
                    tags.add(new JavadocTag(line, col, tagName, content.trim()));
                }
                else {
                    invalidTags.add(new InvalidJavadocTag(line, col, tagName));
                }
            }
            // No block tag, so look for inline validTags
            else if (aTagType.equals(JavadocTagType.ALL) || aTagType.equals(JavadocTagType.INLINE)) {
                // Match JavaDoc text after comment characters
                final Pattern commentPattern = CommonUtil.createPattern("^\\s*(?:/\\*{2,}|\\*+)\\s*(.*)");
                final Matcher commentMatcher = commentPattern.matcher(s);
                final String commentContents;
                final int commentOffset; // offset including comment characters
                if (!commentMatcher.find()) {
                    commentContents = s; // No leading asterisks, still valid
                    commentOffset = 0;
                }
                else {
                    commentContents = commentMatcher.group(1);
                    commentOffset = commentMatcher.start(1) - 1;
                }
                final Pattern tagPattern = CommonUtil.createPattern(".*?\\{@(\\p{Alpha}+)\\s+([^\\}]*)"); // The last '}' may
                // appear on the next
                // line ...
                final Matcher tagMatcher = tagPattern.matcher(commentContents);
                while (tagMatcher.find()) {
                    if (tagMatcher.groupCount() == 2) {
                        final String tagName = tagMatcher.group(1);
                        final String tagValue = tagMatcher.group(2).trim();
                        final int line = aCmt.getStartLineNo() + i;
                        int col = commentOffset + (tagMatcher.start(1) - 1);
                        if (i == 0) {
                            col += aCmt.getStartColNo();
                        }
                        if (JavadocTagInfo.isValidName(tagName)) {
                            tags.add(new JavadocTag(line, col, tagName, tagValue));
                        }
                        else {
                            invalidTags.add(new InvalidJavadocTag(line, col, tagName));
                        }
                    }
                    // else Error: Unexpected match count for inline JavaDoc tag
                }
            }
            blockTagPattern = CommonUtil.createPattern("^\\s*\\**\\s*@(\\p{Alpha}+)\\s");
        }
        return new JavadocTags(tags, invalidTags);
    }
}
