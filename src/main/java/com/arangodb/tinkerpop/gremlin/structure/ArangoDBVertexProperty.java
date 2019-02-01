//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop-Enabled Providers OLTP for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.util.*;

import com.arangodb.velocypack.annotations.Expose;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Class ArangoDBVertexProperty.
 *
 * @param <V> the type of the property value
 * 
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */

public class ArangoDBVertexProperty<V> extends ArangoDBElementProperty<V> implements VertexProperty<V>, ArangoDBElement {

	/** The Logger. */
	
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBVertexProperty.class);

	/** The cardinality of the property */

    private final Cardinality cardinality;

    @Expose(serialize = false, deserialize = false)
    private ArangoDBPropertyManager pManager;

	/**
	 * Instantiates a new arango DB vertex property.
	 *
	 * @param key the key
	 * @param value the value
	 * @param element the element
	 */
	
	public ArangoDBVertexProperty(String key, V value, ArangoDBVertex element, Cardinality cardinality) {
		super(key, value, element);
        pManager = new ArangoDBPropertyManager(this);
        this.cardinality = cardinality;
    }

    @Override
    public Vertex element() {
        return (Vertex) element;
    }

    @Override
    public Object id() {
        return key;
    }

    @Override
    public Graph graph() {
        return element.graph();
    }

    @Override
    public String label() {
        return "VertexProperty";
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        return pManager.properties(propertyKeys);
    }

    @Override
    public Set<String> keys() {
        return pManager.keys();
    }

    @Override
    public <U> Property<U> property(String key) {
        return pManager.property(key);
    }

    @Override
    public <U> Iterator<U> values(String... propertyKeys) {
        return pManager.values(propertyKeys);
    }

    @Override
    public <U> Property<U> property(String key, U value) {
        return pManager.property(key, value);
    }

    public Cardinality getCardinality() {
        return cardinality;
    }

    /**
     * This method is intended for rapid deserialization
     * @return
     */
    public void attachProperties(Collection<ArangoDBElementProperty> properties) {
        this.pManager.attachProperties(properties);
    }


    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
    	return key().hashCode() + value().hashCode();
    }

    @Override
    public void save() {
        this.element.save();
    }

    @Override
    public void removeProperty(ArangoDBElementProperty<?> property) {
        this.pManager.removeProperty(property);
    }

    @Override
    public void graph(ArangoDBGraph graph) {

    }

    @Override
    public void setPaired(boolean paired) {

    }
}