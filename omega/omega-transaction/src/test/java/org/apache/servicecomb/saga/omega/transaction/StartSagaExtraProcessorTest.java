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

import org.apache.servicecomb.pack.common.EventType;
import org.apache.servicecomb.pack.omega.context.IdGenerator;
import org.apache.servicecomb.pack.omega.context.OmegaContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

public class StartSagaExtraProcessorTest {
	private final List<TxEvent> messages = new ArrayList<>();
	private final String globalTxId = UUID.randomUUID().toString();

	private final SagaMessageSender sender = new SagaMessageSender() {
		@Override
		public void onConnected() {
		}

		@Override
		public void onDisconnected() {
		}

		@Override
		public void close() {
		}

		@Override
		public String target() {
			return "UNKNOWN";
		}

		@Override
		public AlphaResponse send(TxEvent event) {
			messages.add(event);
			return new AlphaResponse(false);
		}
	};

	@SuppressWarnings("unchecked")
	private final IdGenerator<String> idGenerator = Mockito.mock(IdGenerator.class);

	private final OmegaContext omegaContext = new OmegaContext(idGenerator);
	private final StartSagaExtraProcessor extraProcessor = new StartSagaExtraProcessor(sender, omegaContext);

	@Before
	public void setUp() throws Exception {
		when(idGenerator.nextId()).thenReturn(globalTxId);
		omegaContext.clear();
	}

	@Test
	public void newGlobalTxIdInSagaStart() throws Throwable {
		extraProcessor.sagaStart(5);
		extraProcessor.sagaEnd();

		TxEvent startedEvent = messages.get(0);

		assertThat(startedEvent.globalTxId(), is(globalTxId));
		assertThat(startedEvent.localTxId(), is(globalTxId));
		assertThat(startedEvent.parentTxId(), is(nullValue()));
		assertThat(startedEvent.type(), is(EventType.SagaStartedEvent));

		TxEvent endedEvent = messages.get(1);

		assertThat(endedEvent.globalTxId(), is(globalTxId));
		assertThat(endedEvent.localTxId(), is(globalTxId));
		assertThat(endedEvent.parentTxId(), is(nullValue()));
		assertThat(endedEvent.type(), is(EventType.SagaEndedEvent));

		assertThat(omegaContext.globalTxId(), is(nullValue()));
		assertThat(omegaContext.localTxId(), is(nullValue()));
	}

	@Test
	public void clearContextOnSagaStartError() throws Throwable {
		RuntimeException oops = new RuntimeException("oops");
		extraProcessor.sagaStart(5);
		extraProcessor.sagaAbort("forTest", oops);

		assertThat(messages.size(), is(2));
		TxEvent event = messages.get(0);

		assertThat(event.globalTxId(), is(globalTxId));
		assertThat(event.localTxId(), is(globalTxId));
		assertThat(event.parentTxId(), is(nullValue()));
		assertThat(event.type(), is(EventType.SagaStartedEvent));

		event = messages.get(1);

		assertThat(event.globalTxId(), is(globalTxId));
		assertThat(event.localTxId(), is(globalTxId));
		assertThat(event.parentTxId(), is(nullValue()));
		assertThat(event.type(), is(EventType.TxAbortedEvent));

		assertThat(omegaContext.globalTxId(), is(nullValue()));
		assertThat(omegaContext.localTxId(), is(nullValue()));
	}

}
