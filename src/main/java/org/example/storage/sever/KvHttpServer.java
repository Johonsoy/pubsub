package org.example.storage.sever;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.example.storage.InMemoryKv;
import org.example.storage.KvInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KvHttpServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KvHttpServer.class);
    private final Server server;

    public KvHttpServer(int port) {
        KvInterface kv = new InMemoryKv();
        this.server = new Server(port);

        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);

        context.addServlet(new ServletHolder(new KvServlet(kv)), "/kv");
        LOGGER.info("Jetty server initialized on port {}", port);
    }

    public void start() throws Exception {
        server.start();
        LOGGER.info("Jetty server started on port {}", server.getURI().getPort());
    }

    public void stop() throws Exception {
        server.stop();
        LOGGER.info("Jetty server stopped");
    }

    public static void main(String[] args) throws Exception {
        // 配置日志级别为 DEBUG
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug");

        int port = 8080;
        KvHttpServer kvServer = new KvHttpServer(port);
        kvServer.start();

        // 保持服务器运行，模拟生产环境
        System.out.println("KV HTTP Server running on port " + port + ". Press Ctrl+C to stop.");
        kvServer.server.join();
    }
}