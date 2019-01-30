package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.velocypack.annotations.SerializedName;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class ArangoTestPropertyWrapper {

  protected String _id;
  @SerializedName("@properties")
  protected ArangoTestPropertyValue propertyValue;
  @SerializedName("@properties2")
  protected List<Map<String, ArangoTestPropertyWrapper>> nestedProperies;

  /**
   * Get the Property ID.
   *
   * @return the id
   */
  public String _id() {
    return _id;
  }


  /**
   * Set the Property's ID.
   *
   * @param id the id
   */
  public void _id(String id) {
    this._id = id;
  }
}
