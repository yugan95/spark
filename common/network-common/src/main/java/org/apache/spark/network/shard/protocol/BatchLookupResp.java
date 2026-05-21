package org.apache.spark.network.shard.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.spark.network.protocol.Encoders;

import java.util.Arrays;
import java.util.Objects;

public class BatchLookupResp extends ShardLookupMessage {

    public final long setId;
    public final int shardId;
    public final byte[] rowsData;

    public BatchLookupResp(long setId, int shardId, byte[] rowsData) {
        this.setId = setId;
        this.shardId = shardId;
        this.rowsData = rowsData;
    }

    @Override
    protected ShardLookupMessage.Type type() {
        return Type.BATCH_LOOKUP_RESP;
    }

    @Override
    public int hashCode() {
        return Objects.hash(setId, shardId) * 31 + Arrays.hashCode(rowsData);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("setId", setId)
                .append("shardId", shardId)
                .append("rows data size", rowsData.length).toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof BatchLookupResp) {
            BatchLookupResp o = (BatchLookupResp) other;
            return setId == o.setId
                    && shardId == o.shardId
                    && Arrays.equals(rowsData, o.rowsData);
        }
        return false;
    }

    @Override
    public int encodedLength() {
        return Long.BYTES /* encoded length of setId */
                + Integer.BYTES /* encoded length of shardId */
                + Encoders.ByteArrays.encodedLength(rowsData); /* encoded length of rowsData */
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeLong(setId);
        buf.writeInt(shardId);
        Encoders.ByteArrays.encode(buf, rowsData);
    }

    public static BatchLookupResp decode(ByteBuf buf) {
        long setId = buf.readLong();
        int shardId = buf.readInt();
        byte[] rowsData = Encoders.ByteArrays.decode(buf);
        return new BatchLookupResp(setId, shardId, rowsData);
    }
}
