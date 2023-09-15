package com.amazonaws.athena.connectors.docdb.schema;

import org.bson.Document;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connectors.util.CloseableIterator;

public interface SchemaProvider {

    SchemaBuilder getSchema(CloseableIterator<Document> documentIterator);
}
