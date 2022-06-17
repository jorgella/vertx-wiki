package com.github.jorgella.wiki;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.templ.freemarker.FreeMarkerTemplateEngine;

public class MainVerticle extends AbstractVerticle {

  private static final String SQL_CREATE_PAGES_TABLE = "create table if not exists Pages (Id integer identity primary key, Name varchar(255) unique, Content clob)";
  private static final String SQL_GET_PAGE = "select Id, Content from Pages where Name = ?";
  private static final String SQL_CREATE_PAGE = "insert into Pages values (NULL, ?, ?)";
  private static final String SQL_SAVE_PAGE = "update Pages set Content = ? where Id = ?";
  private static final String SQL_ALL_PAGES = "select Name from Pages";
  private static final String SQL_DELETE_PAGE = "delete from Pages where Id = ?";

  private static final Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class);

  private JDBCClient dbClient;
  private FreeMarkerTemplateEngine templateEngine;

  @Override
  public void start(final Promise<Void> promise) {
    final var steps = prepareDatabase()
        .compose(v -> startHttpServer());

    steps.setHandler(ar -> {
      if (ar.succeeded()) {
        promise.complete();
      } else {
        promise.fail(ar.cause());
      }
    });
  }

  private Future<Void> prepareDatabase() {
    final Promise<Void> promise = Promise.promise();

    dbClient = JDBCClient.createShared(vertx, new JsonObject()
      .put("url", "jdbc:hsqldb:file:db/wiki")
      .put("driver_class", "org.hsqldb.jdbcDriver")
      .put("max_pool_size", 10));

    dbClient.getConnection(ar -> {
      LOGGER.info("Opennig connection with database");
      if (ar.failed()) {
        LOGGER.error("Could not open a database connection", ar.cause());
        promise.fail(ar.cause());
      } else {
        LOGGER.info("Preparating database");
        final SQLConnection connection = ar.result();
        connection.execute(SQL_CREATE_PAGES_TABLE, create -> {
          if (create.failed()) {
            LOGGER.error("Database preparation error", ar.cause());
            promise.fail(ar.cause());
          } else {
            connection.close();
            LOGGER.info("Database started with success");
            promise.complete();
          }
        });
      }
    });

    return promise.future();
  }

  private Future<Void> startHttpServer() {
    final Promise<Void> promise = Promise.promise();

    final var server = vertx.createHttpServer();

    final var router = Router.router(vertx);
    router.get("/").handler(this::indexHandler);

    final var port = 8080;

    server.requestHandler(router)
        .listen(port, ar -> {
          if (ar.succeeded()) {
            LOGGER.info("HTTP Server is running on port {}", port);
            promise.complete();
          } else {
            LOGGER.error("Could not start a HTTP Server", ar.cause());
            promise.fail(ar.cause());
          }
        });

    return promise.future();
  }

  private void indexHandler(final RoutingContext routingcontext1) {

  }

}
