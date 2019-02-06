package com.arangodb.tinkerpop.gremlin.process.traversal.strategy.optimization;

import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;
import static org.junit.Assert.assertEquals;

import com.arangodb.tinkerpop.gremlin.process.traversal.ArangoGraphStep;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ArangoGraphStepStrategyTest {

  @Parameterized.Parameter(value = 0)
  public Traversal original;

  @Parameterized.Parameter(value = 1)
  public Traversal optimized;

  @Parameterized.Parameter(value = 2)
  public Collection<TraversalStrategy> otherStrategies;

  @Test
  public void doTest() {
    final TraversalStrategies strategies = new DefaultTraversalStrategies();
    strategies.addStrategies(ArangoGraphStepStrategy.instance());
    for (final TraversalStrategy strategy : this.otherStrategies) {
      strategies.addStrategies(strategy);
    }
    this.original.asAdmin().setStrategies(strategies);
    this.original.asAdmin().applyStrategies();
    assertEquals(this.optimized, this.original);
  }

  private static GraphTraversal.Admin<?, ?> g_V(final Object... hasKeyValues) {
    final GraphTraversal.Admin<?, ?> traversal = new DefaultGraphTraversal<>();
    final ArangoGraphStep<Vertex, Vertex> graphStep = new ArangoGraphStep<>(
        new GraphStep<>(traversal, Vertex.class, true));
    for (int i = 0; i < hasKeyValues.length; i = i + 2) {
      graphStep
          .addHasContainer(new HasContainer((String) hasKeyValues[i], (P) hasKeyValues[i + 1]));
    }
    return traversal.addStep(graphStep);
  }

  private static GraphStep<?, ?> V(final Object... hasKeyValues) {
    final ArangoGraphStep<Vertex, Vertex> graphStep = new ArangoGraphStep<>(
        new GraphStep<>(EmptyTraversal
            .instance(), Vertex.class, true));
    for (int i = 0; i < hasKeyValues.length; i = i + 2) {
      graphStep
          .addHasContainer(new HasContainer((String) hasKeyValues[i], (P) hasKeyValues[i + 1]));
    }
    return graphStep;
  }


  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> generateTestParameters() {
    final int LAZY_SIZE = 2500;
    return Arrays.asList(new Object[][]{
        //{__.V().out(), g_V().out(), Collections.emptyList()},
        {__.V().has("name", "marko").out(), g_V("name", eq("marko")).out(), Collections.emptyList()},
        {__.V().has("name", "marko"), g_V("name", eq("marko")), Collections.emptyList()},
        //{__.V().has("name", "marko").next(),g_V("name", eq("marko")).next(), Collections.emptyList()}
    });
  }
}