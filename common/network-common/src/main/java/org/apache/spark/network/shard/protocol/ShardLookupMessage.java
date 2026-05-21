package org.apache.spark.network.shard.protocol;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.apache.spark.network.protocol.Encodable;

public abstract class ShardLookupMessage implements Encodable {

    protected abstract Type type();

    public enum Type {
        BATCH_LOOKUP_REQ(0), BATCH_LOOKUP_RESP(1);

        private final byte id;

        Type(int id) {
            assert id < 128 : "Cannot have more than 128 message types";
            this.id = (byte) id;
        }

        public byte id() {
            return id;
        }
    }

    public static class Decoder {
        public static ShardLookupMessage fromByteBuffer(ByteBuffer msg) {
            ByteBuf buf = Unpooled.wrappedBuffer(msg);
            byte type = buf.readByte();
            switch (type) {
                case 0:
                    return BatchLookupReq.decode(buf);
                case 1:
                    return BatchLookupResp.decode(buf);
                default:
                    throw new IllegalArgumentException("Unknown message type: " + type);
            }
        }
    }

    public ByteBuffer toByteBuffer() {
        ByteBuf buf = Unpooled.buffer(encodedLength() + 1);
        buf.writeByte(type().id());
        encode(buf);
        assert buf.writableBytes() == 0 : "Writable bytes remain: " + buf.writableBytes();
        return buf.nioBuffer();
    }
}
