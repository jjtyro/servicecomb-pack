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

import org.apache.servicecomb.pack.omega.context.CallbackContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompensationMessageHandler implements MessageHandler {
  private static final Logger LOG = LoggerFactory.getLogger(CompensationMessageHandler.class);

  private final SagaMessageSender sender;

  private final CallbackContext context;

  public CompensationMessageHandler(SagaMessageSender sender, CallbackContext context) {
    this.sender = sender;
    this.context = context;
  }

  @Override
  public void onReceive(String globalTxId, String localTxId, String parentTxId, String compensationMethod,
      Object... payloads) {
    try {
      context.apply(globalTxId, localTxId, compensationMethod, payloads);
      sender.send(new TxCompensatedEvent(globalTxId, localTxId, parentTxId, compensationMethod));
    } catch (Throwable e) {
      LOG.error("Process reveived compensation command failed! ", e);
      sender.send(new TxCompensateFailedEvent(globalTxId, localTxId, parentTxId, compensationMethod, e));
    }
  }
}
