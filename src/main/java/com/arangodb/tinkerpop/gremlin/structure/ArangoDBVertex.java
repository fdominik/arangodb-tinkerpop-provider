//////////////////////////////////////////////////////////////////////////////////////////
//
// Implementation of the TinkerPop-Enabled Providers OLTP for ArangoDB
//
// Copyright triAGENS GmbH Cologne and The University of York
//
//////////////////////////////////////////////////////////////////////////////////////////

package com.arangodb.tinkerpop.gremlin.structure;

import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import com.arangodb.tinkerpop.gremlin.client.*;
import org.apache.commons.lang.ArrayUtils;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.arangodb.ArangoCursor;
import com.arangodb.tinkerpop.gremlin.utils.ArangoDBUtil;


/**
 * The ArangoDB vertex class.
 *
 * @author Achim Brandt (http://www.triagens.de)
 * @author Johannes Gocke (http://www.triagens.de)
 * @author Guido Schwab (http://www.triagens.de)
 * @author Horacio Hoyos Rodriguez (@horaciohoyosr)
 */

public class ArangoDBVertex extends ArangoDBBaseDocument implements Vertex, ArangoDBElement {

	/** The Logger. */

	private static final Logger logger = LoggerFactory.getLogger(ArangoDBVertex.class);

	/** All property access is delegated to the property manager */

    protected ArangoDBPropertyManager pManager;


    /**
     * Constructor used for ArabgoDB JavaBeans serialisation.
     */

	public ArangoDBVertex() {
		super();
		pManager = new ArangoDBPropertyManager(this);
	}

	/**
	 * Instantiates a new arango DB vertex.
	 *
	 * @param graph the graph
	 * @param collection the collection
	 */
	public ArangoDBVertex(ArangoDBGraph graph, String collection) {
		this(graph, collection, null);
	}

	/**
	 * Instantiates a new arango DB vertex.
	 *
	 * @param graph the graph
	 * @param collection the collection
	 * @param key the key
	 */
	public ArangoDBVertex(ArangoDBGraph graph, String collection, String key) {
		super(key);
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
		logger.info("remove {}", this._id());
		Map<String, Object> bindVars = new HashMap<>();
		// Remove the Vertex and all incoming/outgoing edges
		ArangoDBQueryBuilder queryBuilder = new ArangoDBQueryBuilder(true);
		queryBuilder.iterateGraph(graph.name(), "v", Optional.of("e"), Optional.empty(),
				Optional.of(1), Optional.empty(), ArangoDBQueryBuilder.Direction.OUT,
				this._id(), bindVars)
			// .append(String.format("    REMOVE v IN '%s'\n", ArangoDBUtil.getCollectioName(graph.name(), ArangoDBUtil.ELEMENT_PROPERTIES_COLLECTION, true)))
			.append(String.format("    REMOVE e IN '%s'\n", ArangoDBUtil.getCollectioName(graph.name(), ArangoDBUtil.ELEMENT_PROPERTIES_EDGE, true)));
		String query = queryBuilder.toString();
		logger.debug("AQL {}", query);
		graph.getClient().executeAqlQuery(query , bindVars, null, this.getClass());

		//Remove vertex
		queryBuilder = new ArangoDBQueryBuilder(false)// the false here is never used, because querybuilder is just appending direct AQL query.
			.append(String.format("REMOVE Document(@startVertex) IN %s", graph.getCollectioName(graph.name(), label())));
		query = queryBuilder.toString();
		logger.debug("AQL {}", query);
		graph.getClient().executeAqlQuery(query , bindVars, null, this.getClass());
	}

	@Override
	public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
		logger.info("addEdge in collection {} to vertex {}", label, inVertex == null ? "?" :inVertex.id());
		ElementHelper.legalPropertyKeyValueArray(keyValues);
		ElementHelper.validateLabel(label);
		if (!graph.edgeCollections().contains(label)) {
			throw new IllegalArgumentException(String.format("Edge label (%s)not in graph (%s) edge collections.", label, graph.name()));
		}
		if (inVertex == null) {
			Graph.Exceptions.argumentCanNotBeNull("vertex");
		}
		Object id;
		ArangoDBEdge edge = null;
		if (ElementHelper.getIdValue(keyValues).isPresent()) {
        	id = ElementHelper.getIdValue(keyValues).get();
        	if (graph.features().edge().willAllowId(id)) {
	        	if (id.toString().contains("/")) {
	        		String fullId = id.toString();
	        		String[] parts = fullId.split("/");
	        		// The collection name is the last part of the full name
	        		String[] collectionParts = parts[0].split("_");
					String collectionName = collectionParts[collectionParts.length-1];
					if (collectionName.contains(label)) {
	        			id = parts[1];
	        			
	        		}
	        	}
        		Matcher m = ArangoDBUtil.DOCUMENT_KEY.matcher((String)id);
        		if (m.matches()) {
        			edge = new ArangoDBEdge(graph, label, this, ((ArangoDBVertex) inVertex), id.toString());
        		}
        		else {
            		throw new ArangoDBGraphException(String.format("Given id (%s) has unsupported characters.", id));
            	}
        	}
        	else {
        		throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
        	}
        }
		else {
			edge = new ArangoDBEdge(graph, label, this, ((ArangoDBVertex) inVertex));
		}
        // The vertex needs to exist before we can attach vertexProperties
		graph.getClient().insertEdge(edge);
        ElementHelper.attachProperties(edge, keyValues);
		return edge;
	}

    @Override
    public <V> VertexProperty<V> property(final String key) {
        return pManager.vertexProperty(key);
    }

	@Override
	public <V> VertexProperty<V> property(
		Cardinality cardinality,
		String key,
		V value,
		Object... keyValues) {
		logger.debug("setting vertex property {} = {} ({})", key, value, keyValues);
		ElementHelper.validateProperty(key, value);
		ElementHelper.legalPropertyKeyValueArray(keyValues);
		Optional<Object> idValue = ElementHelper.getIdValue(keyValues);
		if (idValue.isPresent()) {
            String id = null;
		    logger.debug("");
			if (graph.features().vertex().willAllowId(idValue.get())) {
				id = idValue.get().toString();
				if (id.toString().contains("/")) {
	        		String fullId = id.toString();
	        		String[] parts = fullId.split("/");
	        		// The collection name is the last part of the full name
	        		String[] collectionParts = parts[0].split("_");
					String collectionName = collectionParts[collectionParts.length-1];
					if (collectionName.contains(ArangoDBUtil.ELEMENT_PROPERTIES_COLLECTION)) {
	        			id = parts[1];
	        			
	        		}
	        	}
		        Matcher m = ArangoDBUtil.DOCUMENT_KEY.matcher((String)id);
				if (!m.matches()) {
					throw new ArangoDBGraphException(String.format("Given id (%s) has unsupported characters.", id));
		    	}
			}
			else {
				throw VertexProperty.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
			}
			int idIndex = 0;
            for (int i = 0; i < keyValues.length; i+=2) {
                if (keyValues[i] == T.id) {
                    idIndex = i;
                    break;
                }
            }
            // Remove the id key and value
            keyValues = ArrayUtils.remove(keyValues, idIndex);
            keyValues = ArrayUtils.remove(keyValues, idIndex);
            _key(id);
		}
        VertexProperty<V> p = null;
		if (cardinality.equals(VertexProperty.Cardinality.single)) {
		    p = pManager.vertexProperty(key, value);
            addNestedProperties(p, keyValues);
            ElementHelper.attachProperties(p, keyValues);
        }
        // FIXME This assumes Cardinality is not changed from set to list (and viceversa)
        else {
			p = pManager.vertexProperty(key, value, cardinality);
			Collection<VertexProperty<V>> matches = pManager.propertiesForValue(key, value);
            if (matches.isEmpty()) {
                ElementHelper.attachProperties(p, keyValues);
            }
            else {
                for (VertexProperty<V> m : matches) {
                    p = m;
                    ElementHelper.attachProperties(m, keyValues);
                }
            }
        }
		return p;
	}

    @Override
	public Iterator<Edge> edges(
	    Direction direction,
		String... edgeLabels) {
	    List<String> edgeCollections;
        if (edgeLabels.length == 0) {
            edgeCollections = graph.edgeCollections();
        }
        else {
            edgeCollections = Arrays.stream(edgeLabels)
                    .filter(el -> graph.edgeCollections().contains(el))
                    .collect(Collectors.toList());
            if (edgeCollections.isEmpty()) {
                return Collections.emptyIterator();
            }
        }
		return new ArangoDBIterator<Edge>(graph, graph.getClient().getVertexEdges(graph.name(), this, edgeCollections, direction));
	}


	@Override
	public Iterator<Vertex> vertices(Direction direction,
		String... edgeLabels) {
		// Query will raise an exception if the edge_collection name is not in the graph, so we need
		// to filter out edgeLabels not in the graph.
		List<String> edgeCollections;
		if (edgeLabels.length == 0) {
			edgeCollections = graph.edgeCollections();
		}
		else {
			edgeCollections = Arrays.stream(edgeLabels)
				.filter(el -> graph.edgeCollections().contains(el))
				.collect(Collectors.toList());
			// If edgeLabels was not empty but all were discarded, this means that we should
			// return an empty iterator, i.e. no edges for that edgeLabels exist.
			if (edgeCollections.isEmpty()) {
				return Collections.emptyIterator();
			}
		}
		ArangoCursor<ArangoDBVertex> documentNeighbors = graph.getClient().getDocumentNeighbors(graph.name(), this, edgeCollections, direction, ArangoDBPropertyFilter.empty(), ArangoDBVertex.class);
		return new ArangoDBIterator<Vertex>(graph, documentNeighbors);
	}


	@SuppressWarnings("unchecked")
	@Override
	public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
		logger.debug("Get Properties {}", (Object[])propertyKeys);
		return pManager.vertexProperties(propertyKeys);
    }

	@Override
	public <V> Iterator<V> values(String... propertyKeys) {
		logger.debug("Get Values {}", (Object[])propertyKeys);
		return pManager.values(propertyKeys);
	}

	@Override
    public Set<String> keys() {
        return pManager.keys();
    }

	@Override
	public void save() {
		if (paired) {
			graph.getClient().updateDocument(this);
		}
	}

	@Override
	public void removeProperty(ArangoDBElementProperty<?> property) {
		this.pManager.removeProperty(property);
	}


    public VertexProperty.Cardinality cardinality(String key) {
        return pManager.cardinality(key);
    }

    /**
     * This method is intended for rapid deserialization
     * @return
     */
    public void attachProperties(String key, Collection<ArangoDBVertexProperty> properties) {
        this.pManager.attachVertexProperties(key, properties);
    }

	@Override
    public String toString() {
    	return StringFactory.vertexString(this);
    }

	
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }


    /**
     * Add the nested vertexProperties to the vertex property
     * @param p             the VertexProperty
     * @param keyValues     the pairs of nested key:value to add
     */
    private void addNestedProperties(VertexProperty<?> p, Object[] keyValues) {
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (!keyValues[i].equals(T.id) && !keyValues[i].equals(T.label)) {
                p.property((String)keyValues[i], keyValues[i + 1]);
            }
        }
    }

}

