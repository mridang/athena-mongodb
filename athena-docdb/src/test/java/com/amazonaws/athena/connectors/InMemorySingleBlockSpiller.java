package com.amazonaws.athena.connectors;

import java.util.List;

import org.apache.arrow.vector.types.pojo.Schema;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.google.common.collect.ImmutableList;

/**
 * An implementation of {@link BlockSpiller} that will never spill and uses a single {@link Block}.
 */
public final class InMemorySingleBlockSpiller implements BlockSpiller {

    private final Block block;
    private final ConstraintEvaluator constraintEvaluator;

    public InMemorySingleBlockSpiller(Schema schema, ConstraintEvaluator constraintEvaluator) {
        this(schema, constraintEvaluator, new BlockAllocatorImpl());
    }

    public InMemorySingleBlockSpiller(Schema schema, ConstraintEvaluator constraintEvaluator, BlockAllocator blockAllocator) {
        this.block = blockAllocator.createBlock(schema);
        this.constraintEvaluator = constraintEvaluator;
    }

    @Override
    public Block getBlock() {
        return block;
    }

    @Override
    public boolean spilled() {
        return false;
    }

    @Override
    public List<SpillLocation> getSpillLocations() {
        return ImmutableList.of();
    }

    @Override
    public void close() {
        try {
            block.close();
        } catch (Exception e) {
            throw new IllegalStateException("unable to close block", e);
        }
    }

    @Override
    public void writeRows(RowWriter rowWriter) {
        int rowCount = block.getRowCount();
        block.getRowCount();
        int rowsWritten;
        try {
            rowsWritten = rowWriter.writeRows(block, rowCount);
        } catch (Exception ex) {
            throw (ex instanceof RuntimeException) ? (RuntimeException) ex : new RuntimeException(ex);
        }
        if (rowsWritten > 0) {
            block.setRowCount(rowCount + rowsWritten);
        }
    }

    @Override
    public ConstraintEvaluator getConstraintEvaluator() {
        return constraintEvaluator;
    }
}
