//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop-Enabled Providers OLTP for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import com.arangodb.velocypack.annotations.Expose;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.tinkerpop.gremlin.client.ArangoDBBaseEdge;
import com.arangodb.tinkerpop.gremlin.client.ArangoDBIterator;


/**
 * The ArangoDB Edge class.
 *
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */

public class ArangoDBEdge extends ArangoDBBaseEdge implements Edge, ArangoDBElement {


	/** The Logger. */
	private static final Logger logger = LoggerFactory.getLogger(ArangoDBEdge.class);

	/** All property access is delegated to the property manager */

	protected ArangoDBPropertyManager pManager;

    /**
     * Constructor used for ArabgoDB JavaBeans serialisation.
     */

	public ArangoDBEdge() {
        super();
		pManager = new ArangoDBPropertyManager(this);

    }


	/**
	 * Create a new ArangoDBEdge that connects the given vertices.
	 *
	 * @param graph         the graph in which the edge is created
	 * @param collection    the collection into with the edge is created
	 * @param from          the source vertex
	 * @param to            the target vertex
	 */

	public ArangoDBEdge(
			ArangoDBGraph graph,
			String collection,
			ArangoDBVertex from,
			ArangoDBVertex to) {
		this(graph, collection, from, to, null);
	}


    /**
     * Create a new ArangoDBEdge that connects the given vertices.
     *
     * @param graph         the graph in which the edge is created
     * @param collection    the collection into with the edge is created
     * @param from          the source vertex
     * @param to            the target vertex
     * @param key           the edge key
     */

	public ArangoDBEdge(
	    ArangoDBGraph graph,
        String collection,
        ArangoDBVertex from,
        ArangoDBVertex to,
        String key) {
		super(from._id(), to._id(), key, graph, collection);
        this.graph = graph;
        this.collection = collection;
		pManager = new ArangoDBPropertyManager(this);
	}


    @Override
    public Object id() {
        return _id();
    }

    @Override
    public String label() {
        return collection();
    }

	@Override
	public void remove() {
		logger.info("removing {} from graph {}.", this._key(), graph.name());
		graph.getClient().deleteEdge(this);
	}

	@Override
	public Iterator<Vertex> vertices(Direction direction) {
		boolean from = true;
		boolean to = true;
		switch(direction) {
		case BOTH:
			break;
		case IN:
			from = true;
			break;
		case OUT:
			to = true;
			break;
		}
		return new ArangoDBIterator<>(graph, graph.getClient().getEdgeVertices(graph.name(), _id(), label(), from, to));
	}

	@Override
	public void removeProperty(ArangoDBElementProperty<?> property) {
		pManager.removeProperty(property);
	}

	@Override
	public <V> Property<V> property(final String key) {
		return pManager.property(key);
	}

	@Override
	public <V> Iterator<Property<V>> properties(String... propertyKeys) {
		logger.debug("Get Properties {}", (Object[])propertyKeys);
		return pManager.properties(propertyKeys);
	}

	@Override
	public <V> Iterator<V> values(String... propertyKeys) {
		logger.debug("Get Values {}", (Object[])propertyKeys);
		return pManager.values(propertyKeys);
	}

	@Override
    public <V> Property<V> property(final String key, final V value) {
		Property<V> property = pManager.property(key, value);
		return property;
    }

	@Override
	public void save() {
		if (paired) {
			graph.getClient().updateDocument(this);
		}
	}

	@Override
	public Set<String> keys() {
		return pManager.keys();
	}

	@Override
    public String toString() {
    	return StringFactory.edgeString(this);
    }
	
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

}
