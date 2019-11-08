/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.network;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;

public class BuildInfoServlet extends HttpServlet {
    private static final String CONTENT_TYPE = "application/json";
    private static final String CACHE_CONTROL = "Cache-Control";
    private static final String NO_CACHE = "must-revalidate,no-cache,no-store";
    private static final long serialVersionUID = -3785964478281437018L;
    private Map<String, String> buildInfo;

    private ObjectMapper mapper = new ObjectMapper();

    public BuildInfoServlet(Map<String, String> buildInfo) {
        this.buildInfo = buildInfo;
    }

    @Override
    protected void doGet(HttpServletRequest req,
                         HttpServletResponse resp)
            throws IOException {
        resp.setContentType(CONTENT_TYPE);
        resp.setHeader(CACHE_CONTROL, NO_CACHE);
        resp.setStatus(HttpServletResponse.SC_OK);

        try (PrintWriter writer = resp.getWriter()) {
            StringWriter stringWriter = new StringWriter();
            mapper.writeValue(stringWriter, buildInfo);
            writer.println(stringWriter.toString());
        }
    }
}
