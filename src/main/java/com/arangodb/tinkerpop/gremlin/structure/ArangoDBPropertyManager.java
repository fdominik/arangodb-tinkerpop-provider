package com.arangodb.tinkerpop.gremlin.structure;

import com.arangodb.velocypack.annotations.Expose;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class centralized the management of Element/VertexProperty vertexProperties. Vertices, edges and VertexProperties
 * delegate all property related methods to this class.
 */
public class ArangoDBPropertyManager {

    /** The Logger. */

    private static final Logger logger = LoggerFactory.getLogger(ArangoDBVertex.class);

    /** The element that owns the property */

    private final Element element;

    @Expose(serialize = false, deserialize = false)
    protected Map<String, Collection<ArangoDBElementProperty>> properties = new HashMap<>();

    @Expose(serialize = false, deserialize = false)
    protected Map<String, VertexProperty.Cardinality> cardinalities = new HashMap<>();

    public ArangoDBPropertyManager(Element element) {
        this.element = element;
    }

    public Set<String> keys() {
        return Collections.unmodifiableSet(properties.keySet());
    }

    public void removeProperty(ArangoDBElementProperty<?> property) {

        Collection<ArangoDBElementProperty> props = properties.get(property.key);
        boolean r = props.remove(property);
        if (!r) {
            logger.info("Attempting to remove unknown property %s from element.", property.key);
        }
        if (props.isEmpty()) {
            properties.remove(property.key);
            cardinalities.remove(property.key);
        }
    }

    public <V> Property<V> property(final String key) {
        if (properties.containsKey(key)) {
            ArangoDBElementProperty value = properties.get(key).iterator().next();
            return value;
        }
        else {
            // return new ArangoDBElementProperty<>(key, null, element);
            return Property.empty();
        }
    }

    public <V> VertexProperty<V> vertexProperty(final String key) {
        if (properties.containsKey(key)) {
            ArangoDBVertexProperty value = (ArangoDBVertexProperty) properties.get(key).iterator().next();
            return value;
        }
        else {
            // return new ArangoDBVertexProperty<>(key, null, (ArangoDBVertex) element, cardinalities.get(key));
            return VertexProperty.empty();
        }
    }


    public <V> Iterator<VertexProperty<V>> vertexProperties(String... propertyKeys) {
        String[] keys = allPropertiesIfEmpty(propertyKeys);
        return Arrays.stream(keys)
                .flatMap(this::vertexPropertyValueAsStream)
                .map(p -> (VertexProperty<V>)p)
                .iterator();

    }

    @SuppressWarnings("unchecked")
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        String[] keys = allPropertiesIfEmpty(propertyKeys);
        return Arrays.stream(keys)
                .flatMap(this::propertyValueAsStream)
                .map(p -> (Property<V>)p)
                .iterator();
    }

    public <V> Property<V> property(final String key, final V value) {
        // This method is for Edges and VertexProperties. Thus, we assume {@link VertexProperty.Cardinality#single}
        ArangoDBElementProperty<V> p = new ArangoDBElementProperty<>(key, value, (ArangoDBElement) element);
        addSingleProperty(key, p);
        return p;
    }

    public <V> VertexProperty<V> vertexProperty(final String key, final V value) {
        ArangoDBVertexProperty<V> p = new ArangoDBVertexProperty<>(key, value, (ArangoDBVertex) element, VertexProperty.Cardinality.single);
        addSingleProperty(key, p);
        return p;
    }

    public <V> VertexProperty<V> vertexProperty(final String key, final V value, VertexProperty.Cardinality cardinality) {
        ArangoDBVertexProperty<V> p = new ArangoDBVertexProperty<>(key, value, (ArangoDBVertex) element, cardinality);
        Collection<ArangoDBElementProperty> props = properties.get(key);
        if (props == null) {
            if (VertexProperty.Cardinality.list.equals(cardinality)) {
                props = new ArrayList<>();
            }
            else {
                props = new HashSet<>();
            }
            properties.put(key, props);
            cardinalities.put(key, cardinality);
        }
        props.add(p);
        ((ArangoDBElement) element).save();
        return p;
    }

    @SuppressWarnings("unchecked")
    public <V> Iterator<V> values(String... propertyKeys) {
        String[] keys = allPropertiesIfEmpty(propertyKeys);
        return Arrays.stream(keys)
                .flatMap(this::propertyValueAsStream)
                .map(pv -> ((Property<V>)pv).value())
                .iterator();
    }

    public VertexProperty.Cardinality cardinality(String key) {
        return cardinalities.get(key);
    }

    /**
     * This returns all the vertexProperties that match the key:value pair.
     * @param key
     * @param value
     * @param <V>
     * @return
     */
    public <V> Collection<VertexProperty<V>> propertiesForValue(String key, V value) {
        if (!properties.containsKey(key)) {
            return Collections.emptyList();
        }
        if (VertexProperty.Cardinality.single.equals(cardinalities.get(key))) {
            throw new IllegalStateException("Matching values search can not be used for vertexProperties with SINGLE cardinallity");
        }
        Collection<VertexProperty<V>> result = null;
        if (VertexProperty.Cardinality.list.equals(cardinalities.get(key))) {
            result = new ArrayList<>();
        }
        else if (VertexProperty.Cardinality.set.equals(cardinalities.get(key))) {
            result = new HashSet<>();
        }
        if (result == null) {
            throw new IllegalStateException("Matching values search can not be used for vertexProperties with assigned cardinallity");
        }
        Iterator<? extends Property<Object>> itty = vertexProperties(key);
        while (itty.hasNext()) {
            final VertexProperty<V> property = (VertexProperty<V>) itty.next();
            if (property.value().equals(value)) {
                result.add(property);
            }
        }
        return result;
    }

    public void attachProperties(Collection<ArangoDBElementProperty> properties) {
        for (ArangoDBElementProperty p : properties) {
            this.properties.put(p.key(), Collections.singleton(p));
            this.cardinalities.put(p.key(), VertexProperty.Cardinality.single);
        }
    }

    public void attachVertexProperties(String key, Collection<ArangoDBVertexProperty> properties) {
        this.properties.put(key, properties.stream().map(p -> (ArangoDBElementProperty)p).collect(Collectors.toList()));
        this.cardinalities.put(key, properties.stream().findFirst().map(ArangoDBVertexProperty::getCardinality).orElse(VertexProperty.Cardinality.single));
    }

    private <V> void addSingleProperty(String key, ArangoDBElementProperty<V> p) {
        this.properties.put(key, Collections.singleton(p));
        this.cardinalities.put(key, VertexProperty.Cardinality.single);
        ((ArangoDBElement) element).save();
    }

    private <V> Stream<? extends Property<V>> propertyValueAsStream(final String key) {
        if (properties.containsKey(key)) {
            return properties.get(key).stream().map(p -> (Property<V>)p);
        }
        else {
            //Property<V> p = new ArangoDBElementProperty<>(key, null, (ArangoDBElement) element);
            Property<V> p = Property.empty();
            return Collections.singleton(p).stream();
        }
    }

    private <V> Stream<? extends VertexProperty<V>> vertexPropertyValueAsStream(final String key) {
        if (properties.containsKey(key)) {
            return properties.get(key).stream().map(p -> (VertexProperty<V>)p);
        }
        else {
            return Stream.empty();
        }
    }

    private String[] allPropertiesIfEmpty(String[] propertyKeys) {
        if (propertyKeys.length > 0) {
            return propertyKeys;
        }
        return keys().toArray(new String[0]);
    }

}
