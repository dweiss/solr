/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.search.facet;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import org.apache.solr.core.AbstractSolrEventListener;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FacetFieldPreloader extends AbstractSolrEventListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public FacetFieldPreloader(SolrCore core) {
    super(core);
  }

  @Override
  public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
    log.warn("Forking a background thread to warm up doc value caches.");

    List<ForkJoinTask<?>> tasks = new ArrayList<>();
    ForkJoinPool forkJoinPool = ForkJoinPool.commonPool();
    IndexSchema schema = newSearcher.getSchema();
    for (String fieldName : newSearcher.getFieldNames()) {
      SchemaField schemaField = schema.getFieldOrNull(fieldName);
      if (schemaField != null && fieldName.contains("_facet")) {
        ForkJoinTask<?> task =
            forkJoinPool.submit(
                () -> {
                  try {
                    String result = "preloaded";
                    if (newSearcher.getSlowAtomicReader().getSortedSetDocValues(fieldName)
                        == null) {
                      result = "no doc values";
                    }
                    log.warn(
                        "  Sorted set doc values warmed up for field '"
                            + fieldName
                            + "': "
                            + result
                            + ", dv="
                            + schemaField.hasDocValues()
                            + ", dvAsStored="
                            + schemaField.useDocValuesAsStored());
                  } catch (IOException ex) {
                    log.error("  Could not preload sorted set doc values for: " + fieldName);
                  }
                });
        tasks.add(task);
      }
    }

    for (ForkJoinTask<?> task : tasks) {
      task.join();
    }

    log.warn("Done warming up doc value caches.");
  }
}
