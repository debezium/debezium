/*-------------------------------------------------------------------------
*
* Copyright (c) 2004-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.util;

import java.text.MessageFormat;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * This class provides a wrapper around a gettext message catalog that can provide a localized
 * version of error messages. The caller provides a message String in the standard
 * java.text.MessageFormat syntax and any arguments it may need. The returned String is the
 * localized version if available or the original if not.
 */
public class GT {

  private final static GT _gt = new GT();
  private final static Object noargs[] = new Object[0];

  public static String tr(String message, Object... args) {
    return _gt.translate(message, args);
  }

  private ResourceBundle _bundle;

  private GT() {
    try {
      _bundle = ResourceBundle.getBundle("org.postgresql.translation.messages");
    } catch (MissingResourceException mre) {
      // translation files have not been installed
      _bundle = null;
    }
  }

  private String translate(String message, Object args[]) {
    if (_bundle != null && message != null) {
      try {
        message = _bundle.getString(message);
      } catch (MissingResourceException mre) {
        // If we can't find a translation, just
        // use the untranslated message.
      }
    }

    // If we don't have any parameters we still need to run
    // this through the MessageFormat(ter) to allow the same
    // quoting and escaping rules to be used for all messages.
    //
    if (args == null) {
      args = noargs;
    }

    // Replace placeholders with arguments
    //
    if (message != null) {
      message = MessageFormat.format(message, args);
    }

    return message;
  }
}
