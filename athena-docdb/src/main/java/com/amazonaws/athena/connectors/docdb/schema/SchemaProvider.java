package com.amazonaws.athena.connectors.docdb.schema;

import org.apache.arrow.vector.types.pojo.Schema;
import org.bson.Document;

import com.amazonaws.athena.connectors.util.CloseableIterator;

public interface SchemaProvider {

    Schema getSchema(CloseableIterator<Document> documentIterator);
}
