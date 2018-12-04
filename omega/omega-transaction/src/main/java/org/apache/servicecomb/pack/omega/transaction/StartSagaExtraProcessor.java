/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.servicecomb.pack.omega.transaction;

import org.apache.servicecomb.pack.omega.context.OmegaContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

public class StartSagaExtraProcessor {
	private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private final SagaStartAnnotationProcessor sagaStartAnnotationProcessor;

	private final OmegaContext context;

	public StartSagaExtraProcessor(SagaMessageSender sender, OmegaContext context) {
		this.context = context;
		this.sagaStartAnnotationProcessor = new SagaStartAnnotationProcessor(context, sender);
	}

	public void sagaStart(int timeout) throws Throwable {
		initializeOmegaContext();
		sagaStartAnnotationProcessor.preIntercept(timeout);
		LOG.debug("Transaction with context {} has started.", context);
	}

	public void sagaEnd() throws Throwable {
		try {
			sagaStartAnnotationProcessor.postIntercept(context.globalTxId());
			LOG.debug("Transaction with context {} has finished.", context);
		} catch (Throwable throwable) {
			throw throwable;
		} finally {
			context.clear();
		}
	}

	public void sagaAbort(Throwable throwable)  throws Throwable {
		try {
			if (!(throwable instanceof OmegaException)) {
				sagaStartAnnotationProcessor.onError(null, throwable);
				LOG.error("Transaction {} failed.", context.globalTxId());
			}
		} catch (Throwable throwableNew) {
			throw throwableNew;
		} finally {
			context.clear();
		}
	}

	private void initializeOmegaContext() {
		context.setLocalTxId(context.newGlobalTxId());
	}
}
