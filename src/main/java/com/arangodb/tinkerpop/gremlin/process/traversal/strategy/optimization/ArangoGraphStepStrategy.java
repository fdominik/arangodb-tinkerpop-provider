package com.arangodb.tinkerpop.gremlin.process.traversal.strategy.optimization;

import com.arangodb.tinkerpop.gremlin.process.traversal.ArangoGraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Strategy aims for ArangoDB specifics with the Gremlin language.
 */
public class ArangoGraphStepStrategy extends
    AbstractTraversalStrategy<ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
  private static final Logger logger = LoggerFactory.getLogger(ArangoGraphStepStrategy.class);
  private static final ArangoGraphStepStrategy INSTANCE = new ArangoGraphStepStrategy();

  private ArangoGraphStepStrategy() {
    logger.debug("Arango GraphStep Provider Optimization Strategy will be used.'");
  }

  @Override
  public void apply(Traversal.Admin<?, ?> traversal) {
    if (TraversalHelper.onGraphComputer(traversal))
      return;
    for (GraphStep originalGraphStep : TraversalHelper.getStepsOfClass(GraphStep.class, traversal)) {
      ArangoGraphStep<?, ?> arangoGraphStep = new ArangoGraphStep<>(originalGraphStep);
      /* JanusGraph Strategy */
      //if (originalGraphStep.getIds() == null || originalGraphStep.getIds().length == 0) {
        //Try to optimize for index calls
        //final ArangoGraphStep<?, ?> janusGraphStep = new ArangoGraphStep<>(originalGraphStep);
        //TraversalHelper.replaceStep(originalGraphStep, janusGraphStep, traversal);
        //HasStepFolder.foldInIds(janusGraphStep, traversal);
        //HasStepFolder.foldInHasContainer(janusGraphStep, traversal, traversal);
        //HasStepFolder.foldInOrder(janusGraphStep, janusGraphStep.getNextStep(), traversal, traversal, janusGraphStep.returnsVertex(), null);
        //HasStepFolder.foldInRange(janusGraphStep, JanusGraphTraversalUtil.getNextNonIdentityStep(janusGraphStep), traversal, null);

        //Step<?, ?> currentStep = arangoGraphStep.getNextStep();
     /*   while (true) {
          if (currentStep instanceof OrStep && arangoGraphStep instanceof ArangoGraphStep) {
            for (final Traversal.Admin<?, ?> child : ((OrStep<?>) currentStep).getLocalChildren()) {
              if (!validFoldInHasContainer(child.getStartStep(), false)){
                return;
              }
            }
            ((OrStep<?>) currentStep).getLocalChildren().forEach(t ->localFoldInHasContainer(arangoGraphStep, t.getStartStep(), t, rootTraversal));
            traversal.removeStep(currentStep);
          } else if (currentStep instanceof HasContainerHolder){
            final Iterable<HasContainer> containers = ((HasContainerHolder) currentStep).getHasContainers().stream().map(c -> JanusGraphPredicate.Converter.convert(c)).collect(Collectors.toList());
            if  (validFoldInHasContainer(currentStep, true)) {
              arangoGraphStep.addAll(containers);
              currentStep.getLabels().forEach(arangoGraphStep::addLabel);
              traversal.removeStep(currentStep);
            }
          } else if (!(currentStep instanceof IdentityStep) && !(currentStep instanceof NoOpBarrierStep) && !(currentStep instanceof HasContainerHolder)) {
            break;
          }
          currentStep = currentStep.getNextStep();
        }

      } else {
        //Make sure that any provided "start" elements are instantiated in the current transaction
        final Object[] ids = originalGraphStep.getIds();
        ElementUtils.verifyArgsMustBeEitherIdOrElement(ids);
        if (ids[0] instanceof Element) {
          //GraphStep constructor ensures that the entire array is elements
          final Object[] elementIds = new Object[ids.length];
          for (int i = 0; i < ids.length; i++) {
            elementIds[i] = ((Element) ids[i]).id();
          }
          originalGraphStep.setIteratorSupplier(() -> originalGraphStep.returnsVertex() ?
              ((Graph) originalGraphStep.getTraversal().getGraph().get()).vertices(elementIds) :
              ((Graph) originalGraphStep.getTraversal().getGraph().get()).edges(elementIds));
        }
      }
*/
      /* TinkerGraphStrategy: */
      TraversalHelper.replaceStep(originalGraphStep, arangoGraphStep, traversal);

      Step<?, ?> currentStep = arangoGraphStep.getNextStep();
      while (currentStep instanceof HasStep || currentStep instanceof NoOpBarrierStep) {
        if (currentStep instanceof HasStep) {
          for (HasContainer hasContainer : ((HasContainerHolder) currentStep).getHasContainers()) {
            if (!GraphStep.processHasContainerIds(arangoGraphStep, hasContainer))
              arangoGraphStep.addHasContainer(hasContainer);
          }
          TraversalHelper.copyLabels(currentStep, currentStep.getPreviousStep(), false);
          traversal.removeStep(currentStep);
        }
        currentStep = currentStep.getNextStep();
      }

    }

  }

  public static ArangoGraphStepStrategy instance() {
    return INSTANCE;
  }

  static boolean validFoldInHasContainer(final Step<?, ?>  tinkerpopStep, final boolean defaultValue){
    Step<?, ?> currentStep = tinkerpopStep;
    Boolean toReturn = null;
    while (!(currentStep instanceof EmptyStep)) {
      if (currentStep instanceof HasContainerHolder) {
        final Iterable<HasContainer> containers = ((HasContainerHolder) currentStep).getHasContainers();
        toReturn = toReturn == null ? validJanusGraphHas(containers) : toReturn && validJanusGraphHas(containers);
      } else if (!(currentStep instanceof IdentityStep) && !(currentStep instanceof NoOpBarrierStep) && !(currentStep instanceof RangeGlobalStep) && !(currentStep instanceof OrderGlobalStep)) {
        toReturn = toReturn == null ? false : (toReturn && defaultValue);
        break;
      }
      currentStep = currentStep.getNextStep();
    }
    return Boolean.TRUE.equals(toReturn);
  }

  static boolean validJanusGraphHas(Iterable<HasContainer> hasConts) {
    //FIXME
    for (HasContainer has : hasConts) {
      if (has.getPredicate() instanceof ConnectiveP) {
        //final List<? extends P<?>> predicates = ((ConnectiveP<?>) has.getPredicate()).getPredicates();
        //return predicates.stream().allMatch(p-> validJanusGraphHas(new HasContainer(has.getKey(), p)));
        return true;
      } else {
        //return Converter.supports(has.getBiPredicate());

        return true;
      }
    }
    return false;
  }
}