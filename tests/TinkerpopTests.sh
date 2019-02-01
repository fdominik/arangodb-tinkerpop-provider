#!/usr/bin/env bash
JAVA_HOME = "/Library/Java/JavaVirtualMachines/jdk1.8.0_144.jdk/Contents/Home/bin/java"
# Tinkerpop allows specific tests to be run by setting the GREMLIN_TESTS environment variable.
# Since we only provide "structure" implementation, the following tests are of interest
# "org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest"
# "org.apache.tinkerpop.gremlin.algorithm.generator.DistributionGeneratorTest"
# "org.apache.tinkerpop.gremlin.structure.EdgeTest"
# "org.apache.tinkerpop.gremlin.structure.FeatureSupportTest"
# "org.apache.tinkerpop.gremlin.structure.GraphConstructionTest"
# "org.apache.tinkerpop.gremlin.structure.GraphTest"
# "org.apache.tinkerpop.gremlin.structure.io.IoCustomTest"
# "org.apache.tinkerpop.gremlin.structure.io.IoGraphTest"
# "org.apache.tinkerpop.gremlin.structure.io.IoPropertyTest"
# "org.apache.tinkerpop.gremlin.structure.io.IoTest"
# "org.apache.tinkerpop.gremlin.structure.io.IoVertexTest"
# "org.apache.tinkerpop.gremlin.structure.PropertyTest"
# "org.apache.tinkerpop.gremlin.structure.VariablesTest"
# "org.apache.tinkerpop.gremlin.structure.VertexPropertyTest"
#
GREMLIN_TESTS = "org.apache.tinkerpop.gremlin.algorithm.generator.CommunityGeneratorTest"
