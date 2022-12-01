/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.kafka.connect.sink.namespace.mapping;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.*;
import static com.mongodb.kafka.connect.util.ConfigHelper.documentFromString;
import static java.lang.String.format;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.sink.SinkRecord;

import org.bson.Document;

import com.mongodb.MongoNamespace;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.util.ConnectConfigException;

public class ManualNamespaceMapper extends FieldPathNamespaceMapper {

  private static final String NAMESPACE_WILDCARD = "*";

  private Map<MongoNamespace, MongoNamespace> namespaceMap;

  @Override
  public void configure(final MongoSinkTopicConfig config) {
    super.configure(config);
    String manualNamespaceMappingString = config.getString(MANUAL_NAMESPACE_MAPPER_CONFIG);
    this.namespaceMap = new HashMap<>();

    if (manualNamespaceMappingString.isEmpty()) {
      throw new ConnectConfigException(
          MANUAL_NAMESPACE_MAPPER_CONFIG,
          config.getString(MANUAL_NAMESPACE_MAPPER_CONFIG),
          "Missing configuration for the ManualNamespaceMapper. "
              + "Please configure the manual namespace mapping.");
    }

    Optional<Document> manualNamespaceMapping = documentFromString(manualNamespaceMappingString);
    manualNamespaceMapping.ifPresent(
        mapping -> {
          for (Map.Entry<String, Object> ns : mapping.entrySet()) {
            String oldNamespaceString = ns.getKey();
            String newNamespaceString = mapping.getString(ns);
            MongoNamespace oldNamespace = new MongoNamespace(oldNamespaceString);
            MongoNamespace newNamespace = new MongoNamespace(newNamespaceString);
            validateNamespaceMapping(oldNamespace, newNamespace);
            namespaceMap.put(oldNamespace, newNamespace);
          }
        });
  }

  @Override
  public MongoNamespace getNamespace(final SinkRecord sinkRecord, final SinkDocument sinkDocument) {
    MongoNamespace targetNamespace = super.getNamespace(sinkRecord, sinkDocument);

    // oldDb.oldColl -> newDb.newColl
    if (namespaceMap.containsKey(targetNamespace)) {
      targetNamespace = namespaceMap.get(targetNamespace);
    } else {
      // oldDb.* -> newDb.*
      MongoNamespace collWildcard =
          new MongoNamespace(targetNamespace.getDatabaseName(), NAMESPACE_WILDCARD);
      if (namespaceMap.containsKey(collWildcard)) {
        MongoNamespace wildcardNs = namespaceMap.get(collWildcard);
        targetNamespace =
            new MongoNamespace(wildcardNs.getDatabaseName(), targetNamespace.getCollectionName());
      }
      // Check for Collection wild cards in mapping
      // *.oldColl -> *.newColl
      MongoNamespace dbWildcard =
          new MongoNamespace(NAMESPACE_WILDCARD, targetNamespace.getCollectionName());
      if (namespaceMap.containsKey(dbWildcard)) {
        MongoNamespace wildcardNs = namespaceMap.get(dbWildcard);
        targetNamespace =
            new MongoNamespace(targetNamespace.getDatabaseName(), wildcardNs.getCollectionName());
      }
    }
    return targetNamespace;
  }

  private void validateNamespaceMapping(MongoNamespace oldNs, MongoNamespace newNs) {

    String oldDb = oldNs.getDatabaseName(), oldColl = oldNs.getCollectionName();
    String newDb = newNs.getDatabaseName(), newColl = newNs.getCollectionName();

    // oldDb.oldColl -> newDb.newColl
    if (!oldDb.equals(NAMESPACE_WILDCARD)
        && !newDb.equals(NAMESPACE_WILDCARD)
        && !oldColl.equals(NAMESPACE_WILDCARD)
        && !newColl.equals(NAMESPACE_WILDCARD)) return;

    // oldDb.* -> newDb.*
    if (!oldDb.equals(NAMESPACE_WILDCARD)
        && !newDb.equals(NAMESPACE_WILDCARD)
        && oldColl.equals(NAMESPACE_WILDCARD)
        && newColl.equals(NAMESPACE_WILDCARD)) return;

    // *.oldColl -> *.newColl
    if (oldDb.equals(NAMESPACE_WILDCARD)
        && newDb.equals(NAMESPACE_WILDCARD)
        && !oldColl.equals(NAMESPACE_WILDCARD)
        && !newColl.equals(NAMESPACE_WILDCARD)) return;

    throw new ConnectConfigException(
        MANUAL_NAMESPACE_MAPPER_CONFIG,
        oldNs + "->" + newNs,
        format(
            "Invalid mapping : %s -> %s, Allowed patterns are: "
                + "oldDb.oldColl -> newDb.newColl, "
                + "oldDb.* -> newDb.*, "
                + "*.oldColl -> *.newColl",
            oldNs, newNs));
  }
}
