package com.wepay.riff.metrics.servlets;

import com.fasterxml.jackson.databind.ObjectMapper;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

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
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
    }

    @Override
    protected void doGet(HttpServletRequest req,
                         HttpServletResponse resp) throws IOException {
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
