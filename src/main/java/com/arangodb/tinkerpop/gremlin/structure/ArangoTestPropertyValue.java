package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.velocypack.annotations.SerializedName;

/**
 *
 */
public class ArangoTestPropertyValue<V> {

  /**
   * The property value.
   */
  @SerializedName("@value")
  protected V _value;

  /**
   * The property type
   */
  @SerializedName("@type")
  protected String _type;

  /**
   * Get the Document's Value.
   *
   * @return the value
   */
  public V _value() {
    return _value;
  }

  /**
   * Set the Document's Value.
   *
   * @param value the value
   */
  public void _value(V value) {
    this._value = value;
  }

  /**
   * Get the Document's ArangoDB Type.
   *
   * @return the type
   */
  public String _type() {
    return _type;
  }


  /**
   * Set the Document's ArangoDB Type.
   *
   * @param type the type
   */
  public void _type(String type) {
    this._type = type;
  }


}
