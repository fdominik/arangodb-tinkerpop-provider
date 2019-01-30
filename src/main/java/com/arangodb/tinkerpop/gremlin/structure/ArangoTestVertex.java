package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBBaseDocument;
import com.arangodb.velocypack.annotations.SerializedName;
import java.util.Map;

/**
 *
 */
public class ArangoTestVertex extends ArangoDBBaseDocument {

  /**
   * The propertyValue. String = key
   * Object = either ArangoProperty or a List<Map<String,Object>>
   */
  @SerializedName("@properties")
  Map<String, ArangoTestPropertyWrapper> properties;

}
