/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.benchmark.library;

import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.asm.dataset.ChecksumHashCode;
import org.apache.flink.graph.asm.simple.directed.Simplify;
import org.apache.flink.graph.asm.translate.TranslateGraphIds;
import org.apache.flink.graph.asm.translate.translators.LongValueToStringValue;
import org.apache.flink.graph.asm.translate.translators.LongValueToUnsignedIntValue;
import org.apache.flink.graph.generator.RMatGraph;
import org.apache.flink.graph.generator.random.JDKRandomGeneratorFactory;
import org.apache.flink.graph.generator.random.RandomGenerableFactory;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

public class HITS
extends RMatAlgorithmRunner {

	private static final int EDGE_FACTOR = 8;

	private static final int ITERATIONS = 10;

	@Override
	protected int getInitialScale(int parallelism) {
		return 16;
	}

	@Override
	protected void runInternal(ExecutionEnvironment env, int scale)
			throws Exception {
		// create graph
		RandomGenerableFactory<JDKRandomGenerator> rnd = new JDKRandomGeneratorFactory();

		long vertexCount = 1L << scale;
		long edgeCount = vertexCount * EDGE_FACTOR;

		Graph<LongValue, NullValue, NullValue> graph = new RMatGraph<>(env, rnd, vertexCount, edgeCount)
			.generate();

		// compute result
		String tag = null;
		DataSet hits = null;

		switch(idType) {
			case INT: {
				tag = "i";
				hits = graph
					.run(new TranslateGraphIds<>(new LongValueToUnsignedIntValue()))
					.run(new Simplify<>())
					.run(new org.apache.flink.graph.library.link_analysis.HITS<>(ITERATIONS));
				} break;

			case LONG: {
				tag = "l";
				hits = graph
					.run(new Simplify<>())
					.run(new org.apache.flink.graph.library.link_analysis.HITS<>(ITERATIONS));
				} break;

			case STRING: {
				tag = "s";
				hits = graph
					.run(new TranslateGraphIds<>(new LongValueToStringValue()))
					.run(new Simplify<>())
					.run(new org.apache.flink.graph.library.link_analysis.HITS<>(ITERATIONS));
				} break;
		}

		// TODO: verify checksum

		new ChecksumHashCode<>()
			.run(hits)
			.execute("HITS s" + scale + "e" + EDGE_FACTOR + tag);
	}
}
