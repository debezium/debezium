/*-------------------------------------------------------------------------
*
* Copyright (c) 2008-2014, PostgreSQL Global Development Group
*
*
*-------------------------------------------------------------------------
*/

package org.postgresql.gss;

import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.TextOutputCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

public class GSSCallbackHandler implements CallbackHandler {

  private final String user;
  private final String password;

  public GSSCallbackHandler(String user, String password) {
    this.user = user;
    this.password = password;
  }

  public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
    for (Callback callback : callbacks) {
      if (callback instanceof TextOutputCallback) {
        TextOutputCallback toc = (TextOutputCallback) callback;
        switch (toc.getMessageType()) {
          case TextOutputCallback.INFORMATION:
            System.out.println("INFO: " + toc.getMessage());
            break;
          case TextOutputCallback.ERROR:
            System.out.println("ERROR: " + toc.getMessage());
            break;
          case TextOutputCallback.WARNING:
            System.out.println("WARNING: " + toc.getMessage());
            break;
          default:
            throw new IOException("Unsupported message type: " + toc.getMessageType());
        }
      } else if (callback instanceof NameCallback) {
        NameCallback nc = (NameCallback) callback;
        nc.setName(user);
      } else if (callback instanceof PasswordCallback) {
        PasswordCallback pc = (PasswordCallback) callback;
        if (password == null) {
          throw new IOException("No cached kerberos ticket found and no password supplied.");
        }
        pc.setPassword(password.toCharArray());
      } else {
        throw new UnsupportedCallbackException(callback, "Unrecognized Callback");
      }
    }
  }

}
