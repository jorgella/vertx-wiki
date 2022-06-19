package com.github.jorgella.wiki;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rjeschke.txtmark.Processor;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.freemarker.FreeMarkerTemplateEngine;

public class MainVerticle extends AbstractVerticle {

  private static final String SQL_CREATE_PAGES_TABLE = """
      create table if not exists Pages
      (
        Id integer identity primary key,
        Name varchar(255) unique,
        Content clob
      )
      """;
  private static final String SQL_GET_PAGE = "select Id, Content from Pages where Name = ?";
  private static final String SQL_CREATE_PAGE = "insert into Pages values (NULL, ?, ?)";
  private static final String SQL_SAVE_PAGE = "update Pages set Content = ? where Id = ?";
  private static final String SQL_ALL_PAGES = "select Name from Pages";
  private static final String SQL_DELETE_PAGE = "delete from Pages where Id = ?";

  private static final String EMPTY_PAGE_MARKDOWN = """
      # A new page
      Feel-free to write in Markdown!
      """;

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
    router.get("/wiki/:page").handler(this::pageRenderHandler);
    router.post().handler(BodyHandler.create());
    router.post("/save").handler(this::pageUpdateHandler);
    router.post("/create").handler(this::pageCreateHandler);
    router.post("/delete").handler(this::pageDeletionHandler);

    templateEngine = FreeMarkerTemplateEngine.create(vertx);

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

  private void indexHandler(final RoutingContext context) {
    dbClient.getConnection(car -> {
      if (car.succeeded()) {
        final var connection = car.result();

        connection.query(SQL_ALL_PAGES, res -> {
          dbClient.close();

          if (res.succeeded()) {
            final var pages = res.result()
                .getResults()
                .stream()
                .map(json -> json.getString(0))
                .sorted()
                .collect(Collectors.toList());

            context.put("title", "Wiki home");
            context.put("pages", pages);

            templateEngine.render(context.data(), "templates/index.ftl", ar -> {
              if (ar.succeeded()) {
                context.response().putHeader("Content-Type", "text/html");
                context.response().end(ar.result());
              } else {
                LOGGER.error("error", ar.cause());
                context.fail(ar.cause());
              }
            });
          } else {
            LOGGER.error("error", res.cause());
            context.fail(res.cause());
          }
        });
      } else {
        LOGGER.error("error", car.cause());
        context.fail(car.cause());
      }
    });
  }

  private void pageRenderHandler(final RoutingContext context) {
    final var page = context.request().getParam("page");

    dbClient.getConnection(car -> {
      if (car.succeeded()) {
        final var connection = car.result();
        connection.queryWithParams(SQL_GET_PAGE, new JsonArray().add(page), fetch -> {
          connection.close();

          if (fetch.succeeded()) {
            final var row = fetch.result()
                .getResults()
                .stream()
                .findFirst()
                .orElseGet(() -> new JsonArray().add(-1).add(EMPTY_PAGE_MARKDOWN));

            final var id = row.getInteger(0);
            final var rawContent = row.getString(1);

            context.put("title", page);
            context.put("id", id);
            context.put("newPage", fetch.result().getResults().size() == 0 ? "yes" : "no");
            context.put("rawContent", rawContent);
            context.put("content", Processor.process(rawContent));
            context.put("timestamp", LocalDateTime.now().toString());

            templateEngine.render(context.data(), "templates/page.ftl", ar -> {
              if (ar.succeeded()) {
                context.response().putHeader("Content-Type", "text/html");
                context.response().end(ar.result());
              } else {
                LOGGER.error("error", ar.cause());
                context.fail(ar.cause());
              }
            });
          } else {
            LOGGER.error("error", fetch.cause());
            context.fail(fetch.cause());
          }
        });
      } else {
        LOGGER.error("error", car.cause());
        context.fail(car.cause());
      }
    });
  }

  private void pageUpdateHandler(final RoutingContext context) {
    final var id = context.request().getParam("id");
    final var title = context.request().getParam("title");
    final var markdown = context.request().getParam("markdown");
    final var newPage = "yes".equals(context.request().getParam("newPage"));

    dbClient.getConnection(car -> {
      if (car.succeeded()) {
        final var connection = car.result();
        final var sql = newPage ? SQL_CREATE_PAGE : SQL_SAVE_PAGE;
        final var params = new JsonArray();

        if (newPage) {
          params.add(title).add(markdown);
        } else {
          params.add(markdown).add(id);
        }

        connection.updateWithParams(sql, params, res -> {
          connection.close();
          if (res.succeeded()) {
            context.response().setStatusCode(303);
            context.response().putHeader("Location", "/wiki/" + title);
            context.response().end();
          } else {
            LOGGER.error("error", res.cause());
            context.fail(res.cause());
          }
        });
      } else {
        LOGGER.error("error", car.cause());
        context.fail(car.cause());
      }
    });
  }

  private void pageCreateHandler(final RoutingContext context) {
    final var pageName = context.request().getParam("name");
    final var location = Optional.of(pageName)
        .filter(Objects::nonNull)
        .map(endpoint -> "/wiki/" + endpoint)
        .orElse("/");

    context.response().setStatusCode(303);
    context.response().putHeader("Location", location);
    context.response().end();
  }

  private void pageDeletionHandler(RoutingContext context) {
    final var id = context.request().getParam("id");

    dbClient.getConnection(car -> {
      if (car.succeeded()) {
        final var connection = car.result();

        final var params = new JsonArray();
        params.add(id);

        connection.updateWithParams(SQL_DELETE_PAGE, params, res -> {
          connection.close();
          if (res.succeeded()) {
            context.response().setStatusCode(303);
            context.response().putHeader("Location", "/");
            context.response().end();
          } else {
            LOGGER.error("error", res.cause());
            context.fail(res.cause());
          }
        });
      } else {
        LOGGER.error("error", car.cause());
        context.fail(car.cause());
      }
    });
  }

}
