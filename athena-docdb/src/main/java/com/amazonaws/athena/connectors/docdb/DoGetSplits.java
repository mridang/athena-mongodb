package com.amazonaws.athena.connectors.docdb;

import static com.amazonaws.athena.connectors.docdb.DocDBMetadataHandler.DOCDB_CONN_STR;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;

public interface DoGetSplits {

    /**
     * Since our connector does not support parallel scans we generate a single Split and include the connection details
     * as a property on the split so that the RecordHandler has easy access to it.
     *
     * @see GlueMetadataHandler
     */
    default GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest request) {
        //Every split must have a unique location if we wish to spill to avoid failures
        SpillLocation spillLocation = makeSpillLocation(request);

        //Since our connector does not support parallel reads we return a fixed split.
        return new GetSplitsResponse(request.getCatalogName(),
                Split.newBuilder(spillLocation, makeEncryptionKey())
                        .add(DOCDB_CONN_STR, getConnStr(request))
                        .build());
    }

    String getConnStr(MetadataRequest request);

    EncryptionKey makeEncryptionKey();

    SpillLocation makeSpillLocation(GetSplitsRequest request);
}
