package com.udacity.webcrawler.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonParser;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * A static utility class that loads a JSON configuration file.
 */
public final class ConfigurationLoader {

  private final Path path;

  /**
   * Create a {@link ConfigurationLoader} that loads configuration from the given {@link Path}.
   */
  public ConfigurationLoader(Path path) {
    this.path = Objects.requireNonNull(path);
  }

  /**
   * Loads configuration from this {@link ConfigurationLoader}'s path
   *
   * @return the loaded {@link CrawlerConfiguration}.
   */
  public CrawlerConfiguration load() {
    // TODO: Fill in this method.

    //Read JSON string from variable 'path' by passing it into a reader and returning a CrawlerConfiguration
    //Reader will automatically close after use, which is required for the conditions in read()
    try (Reader reader = Files.newBufferedReader(path)) {
      return read(reader);
    } catch (java.lang.Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Loads crawler configuration from the given reader.
   *
   * @param reader a Reader pointing to a JSON string that contains crawler configuration.
   * @return a crawler configuration
   */
  public static CrawlerConfiguration read(Reader reader) {
    // This is here to get rid of the unused variable warning.
    Objects.requireNonNull(reader);
    // TODO: Fill in this method
    /*
    A hint from the course: Hint: If you get a "Stream closed" failure in the test,
    try calling ObjectMapper#disable(Feature) to disable the com.fasterxml.jackson.core.JsonParser.Feature.AUTO_CLOSE_SOURCE.
    This prevents the Jackson library from closing the input Reader,
    which you should have already closed in ConfigurationLoader#load()
     */
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);
    try {
      CrawlerConfiguration crawlerConfiguration = objectMapper.readValue(reader, CrawlerConfiguration.Builder.class).build();
      return crawlerConfiguration;
    } catch (java.lang.Exception e) {
      throw new RuntimeException(e);
    }
  }
}
