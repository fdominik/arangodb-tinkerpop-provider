package com.arangodb.tinkerpop.gremlin.structure;

import static org.junit.Assert.assertEquals;

import com.arangodb.velocypack.VPack;
import com.arangodb.velocypack.VPackParser;
import com.arangodb.velocypack.VPackSlice;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class VPackTestOfNestedProperties {

  private final static org.slf4j.Logger LOGGER = LoggerFactory
      .getLogger(VPackTestOfNestedProperties.class);

  @Test
  public void testSerializeVertex() throws Exception {

    ArangoTestVertex atv = new ArangoTestVertex();

    Map<String, ArangoTestPropertyWrapper> mapOfProperties = new HashMap<>();
    ArangoTestPropertyWrapper name = new ArangoTestPropertyWrapper();
    name._id = "1";
    ArangoTestPropertyValue<String> nameValue = new ArangoTestPropertyValue();
    nameValue._type("String");
    nameValue._value("John");
    name.propertyValue = nameValue;
    mapOfProperties.put("name", name);
    ArangoTestPropertyWrapper age = new ArangoTestPropertyWrapper();
    List<Map<String, ArangoTestPropertyWrapper>> nestedAge = new ArrayList<>();

    Map<String, ArangoTestPropertyWrapper> ageOf29 = new HashMap<>();
    ArangoTestPropertyWrapper ageOf29Value = new ArangoTestPropertyWrapper();
    ageOf29Value._id("1");
    ageOf29Value.propertyValue = new ArangoTestPropertyValue<Integer>();
    ageOf29Value.propertyValue._value(29);
    ageOf29Value.propertyValue._type("Integer");
    ageOf29.put("age",ageOf29Value);
    nestedAge.add(ageOf29);

    Map<String, ArangoTestPropertyWrapper> ageOf18 = new HashMap<>();
    ArangoTestPropertyWrapper ageOf18Value = new ArangoTestPropertyWrapper();
    ageOf18Value._id("2");
    ageOf18Value.propertyValue = new ArangoTestPropertyValue<Integer>();
    ageOf18Value.propertyValue._value(18);
    ageOf18Value.propertyValue._type("Integer");
    ageOf18.put("age",ageOf18Value);
    nestedAge.add(ageOf18Value);

    age.nestedProperies = nestedAge;
    mapOfProperties.put("age", age);
    atv.properties = mapOfProperties;
    atv._id("P/1");
    atv._rev("AXZ");
    atv._key("1");

    VPack vpack = new VPack.Builder().build();
    VPackSlice slice = vpack.serialize(atv);

    VPackParser parser = new VPackParser.Builder().build();
    String json = parser.toJson(slice);

    String expectedJson = loadJsonFromFile("vertex-example-with-property.txt");

    assertEquals(expectedJson, json);
  }

  private String loadJsonFromFile(String filename) throws FileNotFoundException {
    LOGGER.debug("Loading json file {} from.", filename);
    String externalDicrectory = "./";
    //whether file should be read from inside of jar file, or from external path

    boolean internalRead = false;
    BufferedReader reader = null;
    Path externalPath = Paths.get(externalDicrectory);
    Class classOfFile = this.getClass();
    //
    LOGGER.debug("External path {} from property file.", externalPath);

    if (externalPath.toString().isEmpty()) {
      internalRead = true;
    } else if (!Files.exists(externalPath)) {
      LOGGER.debug("External path {} doesn't exist.", externalPath);
      internalRead = true;
    } else {
      LOGGER.debug("External path {} exists.", externalPath);
      externalPath = externalPath.resolve(filename);
      if (Files.exists(externalPath)) {
        LOGGER.debug("External file {} in directory {} exists.", externalPath.getFileName(),
            externalPath.getParent());
        try {
          reader =
              new BufferedReader(new InputStreamReader(new FileInputStream(externalPath.toFile()),
                  StandardCharsets.UTF_8));
        } catch (NullPointerException e) {
          String msg = "File " + externalPath.toString() + " wasn't found. ";
          LOGGER.error(msg, e);
          throw new FileNotFoundException(msg);
        } catch (IOException e) {
          String msg = "File " + externalPath.toString() + " couldn't be loaded. ";
          LOGGER.error(msg, e);
          throw new FileNotFoundException(msg);
        }
      } else {
        LOGGER.debug("External file {} in directory {} doesn't exist.", externalPath.getFileName(),
            externalPath.getParent());
        internalRead = true;
      }
    }

    if (internalRead == true) {
      LOGGER.debug("Loading File from the internal path: {}", filename);
      try {
        reader = new BufferedReader(new InputStreamReader(
            classOfFile.getClassLoader().getResource(filename).openStream(),
            StandardCharsets.UTF_8));
      } catch (NullPointerException e) {
        String msg = "File " + filename + " wasn't found.";
        LOGGER.error(msg, e);
        throw new FileNotFoundException(msg);
      } catch (IOException e) {
        String msg = "File " + filename + " couldn't be loaded.";
        LOGGER.error(msg, e);
        throw new FileNotFoundException(msg);
      }
    }

    String fileContent = reader.lines().collect(Collectors.joining());
    return fileContent;

  }

}