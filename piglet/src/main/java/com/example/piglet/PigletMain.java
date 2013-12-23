package com.example.piglet;

import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import org.apache.log4j.Logger;
import org.mortbay.jetty.Server;
import com.sun.jersey.spi.container.servlet.ServletContainer;

public class PigletMain {


  private static final Logger LOGGER = Logger.getLogger(PigletMain.class.getName());

  private static Server server;

  public static void main(String[] args) {
    //daemonize();
    startServer();
  }

  public static void startServer() {
    LOGGER.info("Starting jetty server...");
    try {
      startJettyServer();
    } catch (Exception e) {
      LOGGER.error("Could not start the server");
      e.printStackTrace();
      System.exit(1);
    }
  }

  public static void stopServer() throws Exception {
    if (server != null) {
      server.stop();
    }
  }

  // Made public for testing
  public static void startJettyServer() throws Exception {
    server = new Server(9003);
    ServletHolder sh = new ServletHolder(ServletContainer.class);

    sh.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", "com.sun.jersey.api.core.PackagesResourceConfig");
    sh.setInitParameter("com.sun.jersey.config.property.packages", "com.inmobi.piglet.rest");

    Context context = new Context(server, "/", Context.SESSIONS);
    context.addServlet(sh, "/*");
    server.start();
    server.join();
  }

  static public void daemonize() {
    // getPidFile().deleteOnExit();
    System.out.close();
    System.err.close();
  }

}
