/*-
 * #%L
 * athena-docdb
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.docdb.schema;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.bson.Document;
import org.junit.Test;

import com.amazonaws.athena.connectors.docdb.UnsupportedType;
import com.amazonaws.athena.connectors.util.CloseableIterator;

public class DefaultSchemaProviderTest {

    private final SchemaProvider schemaProvider = new DefaultSchemaProvider();

    @Test
    public void UnsupportedTypeTest() {

        List<Document> docs = new ArrayList<>();
        Document unsupported = new Document();
        unsupported.put("unsupported_col1", new UnsupportedType());
        docs.add(unsupported);

        Schema schema = schemaProvider.getSchema(new CloseableIterator<>() {
            private final Iterator<Document> documentIterator = docs.iterator();

            @Override
            public void close() {
                //
            }

            @Override
            public boolean hasNext() {
                return documentIterator.hasNext();
            }

            @Override
            public Document next() {
                return documentIterator.next();
            }
        });
        assertEquals(1, schema.getFields().size());

        Map<String, Field> fields = new HashMap<>();
        schema.getFields().forEach(next -> fields.put(next.getName(), next));

        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("unsupported_col1").getType()));
    }

    @SuppressWarnings("ExtractMethodRecommender")
    @Test
    public void basicMergeTest() {
        List<String> list = new ArrayList<>();
        list.add("test");
        list.add("test");
        list.add("test");

        Document struct1 = new Document();
        struct1.put("struct_col1", 1);
        struct1.put("struct_col2", "string");
        struct1.put("struct_col3", 1.0D);

        Document struct2 = new Document();
        struct2.put("struct_col1", 1);
        struct2.put("struct_col2", "string");
        struct2.put("struct_col3", 1);
        struct2.put("struct_col4", 2.0F);

        List<Document> docs = new ArrayList<>();
        Document doc1 = new Document();
        doc1.put("col1", 1);
        doc1.put("col2", "string");
        doc1.put("col3", 1.0D);
        doc1.put("col5", list);
        doc1.put("col6", struct1);
        docs.add(doc1);

        Document doc2 = new Document();
        doc2.put("col1", 1);
        doc2.put("col2", "string");
        doc2.put("col4", 1.0F);
        doc2.put("col6", struct2);
        docs.add(doc2);

        Document doc3 = new Document();
        doc3.put("col1", 1);
        doc3.put("col2", "string");
        doc3.put("col4", 1);
        doc3.put("col5", list);
        docs.add(doc3);

        Schema schema = schemaProvider.getSchema(new CloseableIterator<>() {
            private final Iterator<Document> documentIterator = docs.iterator();

            @Override
            public void close() {
                //
            }

            @Override
            public boolean hasNext() {
                return documentIterator.hasNext();
            }

            @Override
            public Document next() {
                return documentIterator.next();
            }
        });
        assertEquals(6, schema.getFields().size());

        Map<String, Field> fields = new HashMap<>();
        schema.getFields().forEach(next -> fields.put(next.getName(), next));

        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(fields.get("col1").getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col2").getType()));
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(fields.get("col3").getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col4").getType()));
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(fields.get("col5").getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col5").getChildren().get(0).getType()));
        assertEquals(Types.MinorType.STRUCT, Types.getMinorTypeForArrowType(fields.get("col6").getType()));
        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(fields.get("col6").getChildren().get(0).getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col6").getChildren().get(1).getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col6").getChildren().get(2).getType()));
        assertEquals(Types.MinorType.FLOAT4, Types.getMinorTypeForArrowType(fields.get("col6").getChildren().get(3).getType()));
    }

    @SuppressWarnings("ExtractMethodRecommender")
    @Test
    public void emptyListTest() {
        List<Document> docs = new ArrayList<>();
        Document doc1 = new Document();
        List<String> emptyList = new ArrayList<>();
        doc1.put("col1", 1);
        doc1.put("col2", "string");
        doc1.put("col3", 1.0D);
        doc1.put("col4", emptyList);
        docs.add(doc1);

        Document doc2 = new Document();
        List<Integer> list2 = new ArrayList<>();
        list2.add(100);
        doc2.put("col1", 1);
        doc2.put("col2", "string");
        doc2.put("col3", 1.0D);
        doc2.put("col4", list2);
        docs.add(doc2);

        Schema schema = schemaProvider.getSchema(new CloseableIterator<>() {
            private final Iterator<Document> documentIterator = docs.iterator();

            @Override
            public void close() {
                //
            }

            @Override
            public boolean hasNext() {
                return documentIterator.hasNext();
            }

            @Override
            public Document next() {
                return documentIterator.next();
            }
        });
        assertEquals(4, schema.getFields().size());

        Map<String, Field> fields = new HashMap<>();
        schema.getFields().forEach(next -> fields.put(next.getName(), next));

        assertEquals(Types.MinorType.INT, Types.getMinorTypeForArrowType(fields.get("col1").getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col2").getType()));
        assertEquals(Types.MinorType.FLOAT8, Types.getMinorTypeForArrowType(fields.get("col3").getType()));
        assertEquals(Types.MinorType.LIST, Types.getMinorTypeForArrowType(fields.get("col4").getType()));
        assertEquals(Types.MinorType.VARCHAR, Types.getMinorTypeForArrowType(fields.get("col4").getChildren().get(0).getType()));
    }
}
