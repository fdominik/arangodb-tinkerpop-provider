package com.arangodb.tinkerpop.gremlin.process.traversal;

import com.arangodb.tinkerpop.gremlin.structure.ArangoDBGraph;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ArangoGraphStep<S, E extends Element> extends GraphStep<S, E> implements
    HasContainerHolder {

  private static final Logger logger = LoggerFactory.getLogger(ArangoGraphStep.class);
  private final List<HasContainer> hasContainers = new ArrayList<>();

  public ArangoGraphStep(final GraphStep<S, E> originalGraphStep) {
    super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(),
        originalGraphStep.isStartStep(), originalGraphStep.getIds());
    originalGraphStep.getLabels().forEach(this::addLabel);
    // used to only setIteratorSupplier() if there were no ids OR the first id was instanceof Element,
    // but that allowed the filter in g.V(v).has('k','v') to be ignored.  this created problems for
    // PartitionStrategy which wants to prevent someone from passing "v" from one TraversalSource to
// another TraversalSource using a different partition
    this.setIteratorSupplier(
        () -> (Iterator<E>) (
            Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()
        )
    );
  }

  private ArangoDBGraph getArangoGraph() {
    Graph baseGraph = this.getTraversal().getGraph().get();
    return (ArangoDBGraph) baseGraph;
  }

  private Iterator<? extends Vertex> vertices() {
    return lookupVertices(getArangoGraph(), this.hasContainers, this.ids);
  }

  /**
   * @param graph ArangoDBGraph graph instance from the Traversal
   * @param ids List of IDs which might be specified in V() or E()
   * @return List of vertices filtered by hasContainers
   */
  private Iterator<? extends Vertex> lookupVertices(final ArangoDBGraph graph,
      final List<HasContainer> hasContainers, final Object... ids) {
    final HasContainer indexedContainer = getIndexKey(Vertex.class);

    // ids are present, filter on them first
    if (null == this.ids) {
      return Collections.emptyIterator();
    } else if (ids.length > 0) {
      //ids to be filtered on are specified, filter the vertices:
      return this.iteratorList(graph.vertices(ids));//this.ids was in TinkerPopStep
      //from bitsy
      //return IteratorUtils
      //  .filter(graph.vertices(ids), vertex -> HasContainer.testAll(vertex, hasContainers));
    } else {
      if (null == indexedContainer) {
        //no vertex collections, return all vertices (filtered by IDs).
        return this.iteratorList(graph.vertices(ids));
      } else {
        List<String> cols = new ArrayList<>();
        for (final HasContainer hasContainer : hasContainers) {
          cols.add(hasContainer.getKey());
        }
        //if (Compare.eq == hasContainer.getBiPredicate() && !hasContainer.getKey()
        //  .equals(T.label.getAccessor())) {
        //if (graph.getIndexedKeys(Vertex.class).contains(hasContainer.getKey())) {
// Find a vertex by key/value
        List<String> idsList = Arrays.stream(ids)
            .map(id -> {
              if (id instanceof Vertex) {
                //logger.trace("Getting Vertex label to be added to VertexCollection.");
                //vertexCollections.add(((Vertex)id).label());
                return ((Vertex) id).id();
              } else {
                // We only support String ids
                return id;
              }
            })
            .map(id -> id == null ? (String) id : id.toString())
            .collect(Collectors.toList());
        return graph.getClient().getGraphVertices(graph, idsList, cols);
        //return IteratorUtils.filter(graph.(ids),
        //  vertex -> HasContainer.testAll(, this.hasContainers));
        //}

      }
    }

    // get a label being search on
    /*
      Optional<String> label = hasContainers.stream()
          .filter(hasContainer -> hasContainer.getKey().equals(T.label.getAccessor()))
          .filter(hasContainer -> Compare.eq == hasContainer.getBiPredicate())
          .map(hasContainer -> (String) hasContainer.getValue())
          .findAny();
*/
    // Labels aren't indexed in Bitsy, only keys -- so do a full scan
    /*
      for (final HasContainer hasContainer : hasContainers) {
        if (Compare.eq == hasContainer.getBiPredicate() && !hasContainer.getKey()
            .equals(T.label.getAccessor())) {
          if (graph.getIndexedKeys(Vertex.class).contains(hasContainer.getKey())) {
            // Find a vertex by key/value
            return IteratorUtils
                .stream(graph.verticesByIndex(hasContainer.getKey(), hasContainer.getValue()))
                .map(vertex -> (Vertex) vertex)
                .filter(vertex -> HasContainer.testAll(vertex, hasContainers)).iterator();
          }
        }
      }
      IteratorUtils
          .filter(
              TinkerHelper.queryVertexIndex(graph, indexedContainer.getKey(),
                  indexedContainer.getPredicate().getValue()).iterator(),
              vertex -> HasContainer.testAll(vertex, this.hasContainers)
          );
      return IteratorUtils
          .filter(
              graph.vertices(),
              vertex -> HasContainer.testAll(vertex, hasContainers)
          );

    } */
  }

  private Iterator<? extends Edge> edges() {
    //return IteratorUtils.filter(this.getTraversal().getGraph().get().edges(this.ids), edge -> HasContainer.testAll(edge, this.hasContainers));
    //  return lookupEdges(getArangoGraph(), this.hasContainers, this.ids);
    return null;
  }

  /*
    private Iterator<Edge> lookupEdges(final ArangoGraphStep graph,
        final List<HasContainer> hasContainers, final Object... ids) {
      // ids are present, filter on them first
      if (ids.length > 0) {
        return IteratorUtils
            .filter(graph.edges(ids), vertex -> HasContainer.testAll(vertex, hasContainers));
      }

      // get a label being search on
      Optional<String> label = hasContainers.stream()
          .filter(hasContainer -> hasContainer.getKey().equals(T.label.getAccessor()))
          .filter(hasContainer -> Compare.eq == hasContainer.getBiPredicate())
          .map(hasContainer -> (String) hasContainer.getValue())
          .findAny();

      // Labels aren't indexed in Bitsy, only keys -- so do a full scan
      for (final HasContainer hasContainer : hasContainers) {
        if (Compare.eq == hasContainer.getBiPredicate() && !hasContainer.getKey()
            .equals(T.label.getAccessor())) {
          if (graph.getIndexedKeys(Vertex.class).contains(hasContainer.getKey())) {
            // Find a vertex by key/value
            return IteratorUtils
                .stream(graph.edgesByIndex(hasContainer.getKey(), hasContainer.getValue()))
                .map(edge -> (Edge) edge)
                .filter(edge -> HasContainer.testAll(edge, hasContainers)).iterator();
          }
        }
      }
      return IteratorUtils
          .filter(graph.edges(), vertex -> HasContainer.testAll(vertex, hasContainers));
    }
  */
  @Override
  public String toString() {
    if (this.hasContainers.isEmpty()) {
      return super.toString();
    } else {
      return 0 == this.ids.length ?
          StringFactory
              .stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.hasContainers)
          :
              StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(),
                  Arrays.toString(this.ids), this.hasContainers);
    }
  }

  @Override
  public List<HasContainer> getHasContainers() {
    return Collections.unmodifiableList(this.hasContainers);
  }

  @Override
  public void addHasContainer(final HasContainer hasContainer) {
    if (hasContainer.getPredicate() instanceof AndP) {
      for (final P<?> predicate : ((AndP<?>) hasContainer.getPredicate()).getPredicates()) {
        this.addHasContainer(new HasContainer(hasContainer.getKey(), predicate));
      }
    } else {
      this.hasContainers.add(hasContainer);
    }
  }

  @Override
  public int hashCode() {
    return super.hashCode() ^ this.hasContainers.hashCode();
  }

  private <E extends Element> Iterator<E> iteratorList(final Iterator<E> iterator) {
    final List<E> list = new ArrayList<>();
    while (iterator.hasNext()) {
      final E e = iterator.next();
      if (HasContainer.testAll(e, this.hasContainers)) {
        list.add(e);
      }
    }
    return list.iterator();
  }

  private HasContainer getIndexKey(final Class<? extends Element> indexedClass) {
    //Get list of collections (either vertex names or edges names).
    final Set<String> indexedCollectionName = ((ArangoDBGraph) this.getTraversal().getGraph().get())
        .getIndexedKeys(indexedClass);

    //
    final Iterator<HasContainer> itty = IteratorUtils.filter(hasContainers.iterator(),
        c -> c.getPredicate().getBiPredicate() == Compare.eq && indexedCollectionName
            .contains(c.getKey())
    );
    return itty.hasNext() ? itty.next() : null;

  }
}
