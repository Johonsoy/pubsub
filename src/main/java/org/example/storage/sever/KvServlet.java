package org.example.storage.sever;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.example.storage.KvInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

class KvServlet extends HttpServlet {
    private static final Logger LOGGER = LoggerFactory.getLogger(KvServlet.class);
    private final KvInterface kv;

    public KvServlet(KvInterface kv) {
        this.kv = kv;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("application/json; charset=UTF-8");
        String key = req.getParameter("key");

        if (key == null || key.trim().isEmpty()) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp.getWriter().write("{\"error\": \"Missing or empty 'key' parameter\"}");
            LOGGER.warn("GET request failed: missing or empty key");
            return;
        }

        byte[] value = kv.get(key);
        if (value == null) {
            resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
            resp.getWriter().write("{\"error\": \"Key not found\"}");
            LOGGER.info("GET key={} not found", key);
        } else {
            String valueStr = new String(value, StandardCharsets.UTF_8);
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().write(String.format("{\"key\": \"%s\", \"value\": \"%s\"}", key, valueStr));
            LOGGER.info("GET key={} returned value={}", key, valueStr);
        }
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("application/json; charset=UTF-8");
        String key = req.getParameter("key");
        String value = req.getParameter("value");

        if (key == null || key.trim().isEmpty()) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp.getWriter().write("{\"error\": \"Missing or empty 'key' parameter\"}");
            LOGGER.warn("PUT request failed: missing or empty key");
            return;
        }
        if (value == null || value.trim().isEmpty()) {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            resp.getWriter().write("{\"error\": \"Missing or empty 'value' parameter\"}");
            LOGGER.warn("PUT request failed: missing or empty value for key={}", key);
            return;
        }

        kv.set(key, value.getBytes(StandardCharsets.UTF_8));
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.getWriter().write("{\"status\": \"success\"}");
        LOGGER.info("PUT key={} value={}", key, value);
    }
}
