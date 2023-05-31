/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.ComponentScan;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Execute entry into Data Prepper.
 */
@ComponentScan
public class DataPrepperExecute {
    //VARIABLES MODIFIED FOR TESTING:
    private static final boolean TESTING = true; //TODO: REMOVE AFTER TESTING IS DONE.
    private static final String DATAPREPPERHOME = "/Users/alemayed/Amazon_Projects/CWL_SINK/forked_repo/data-prepper/release/archives/linux/build/install/opensearch-data-prepper-2.3.0-SNAPSHOT-linux-x64/bin";
    private static final Logger LOG = LoggerFactory.getLogger(DataPrepperExecute.class);
    public static void main(final String ... args) {
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");
        System.setProperty("software.amazon.awssdk.http.service.impl", "software.amazon.awssdk.http.apache.ApacheSdkHttpService");

        final ContextManager contextManager;
        if (TESTING) { //ADDED FOR TESTING: ****************************************************************************************************
            final String dataPrepperPipelines = Paths.get(DATAPREPPERHOME + "/..").resolve("pipelines/").toString();
            final String dataPrepperConfig = Paths.get(DATAPREPPERHOME + "/..").resolve("config/data-prepper-config.yaml").toString();
            contextManager = new ContextManager(dataPrepperPipelines, dataPrepperConfig); //ADDED FOR TESTING ****************************************************************************************************
        } else if (args.length == 0) {
            final String dataPrepperHome = System.getProperty("data-prepper.dir");
            if (dataPrepperHome == null) {
                throw new RuntimeException("Data Prepper home directory (data-prepper.dir) not set in system properties.");
            }
            final String dataPrepperPipelines = Paths.get(dataPrepperHome).resolve("pipelines/").toString();
            final String dataPrepperConfig = Paths.get(dataPrepperHome).resolve("config/data-prepper-config.yaml").toString();
            contextManager = new ContextManager(dataPrepperPipelines, dataPrepperConfig);
        } else {
            contextManager = new ContextManager(args);
        }

        final DataPrepper dataPrepper = contextManager.getDataPrepperBean();

        LOG.trace("Starting Data Prepper execution");
        dataPrepper.execute();
    }
}
