/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.flight;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.flight.grpc.AddWritableBuffer;
import org.apache.arrow.flight.grpc.GetReadableBuffer;
import org.apache.arrow.flight.impl.Flight.FlightData;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.message.ArrowBodyCompression;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageMetadataResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.MetadataVersion;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;

import io.grpc.Drainable;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.protobuf.ProtoUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

/**
 * The in-memory representation of FlightData used to manage a stream of Arrow messages.
 */
class ArrowMessage implements AutoCloseable {

  public static final boolean FAST_PATH = true;

  private static final int DESCRIPTOR_TAG =
      (FlightData.FLIGHT_DESCRIPTOR_FIELD_NUMBER << 3) | WireFormat.WIRETYPE_LENGTH_DELIMITED;
  private static final int BODY_TAG =
      (FlightData.DATA_BODY_FIELD_NUMBER << 3) | WireFormat.WIRETYPE_LENGTH_DELIMITED;
  private static final int HEADER_TAG =
      (FlightData.DATA_HEADER_FIELD_NUMBER << 3) | WireFormat.WIRETYPE_LENGTH_DELIMITED;
  private static final int APP_METADATA_TAG =
      (FlightData.APP_METADATA_FIELD_NUMBER << 3) | WireFormat.WIRETYPE_LENGTH_DELIMITED;

  private static final Marshaller<FlightData> NO_BODY_MARSHALLER =
      ProtoUtils.marshaller(FlightData.getDefaultInstance());

  /** Get the application-specific metadata in this message. The ArrowMessage retains ownership of the buffer. */
  public ArrowBuf getApplicationMetadata() {
    return appMetadata;
  }

  /** Types of messages that can be sent. */
  public enum HeaderType {
    NONE,
    SCHEMA,
    DICTIONARY_BATCH,
    RECORD_BATCH,
    TENSOR
    ;

    public static HeaderType getHeader(byte b) {
      switch (b) {
        case 0: return NONE;
        case 1: return SCHEMA;
        case 2: return DICTIONARY_BATCH;
        case 3: return RECORD_BATCH;
        case 4: return TENSOR;
        default:
          throw new UnsupportedOperationException("unknown type: " + b);
      }
    }

  }

  // Pre-allocated buffers for padding serialized ArrowMessages.
  private static final List<ByteBuf> PADDING_BUFFERS = Arrays.asList(
      null,
      Unpooled.copiedBuffer(new byte[] { 0 }),
      Unpooled.copiedBuffer(new byte[] { 0, 0 }),
      Unpooled.copiedBuffer(new byte[] { 0, 0, 0 }),
      Unpooled.copiedBuffer(new byte[] { 0, 0, 0, 0 }),
      Unpooled.copiedBuffer(new byte[] { 0, 0, 0, 0, 0 }),
      Unpooled.copiedBuffer(new byte[] { 0, 0, 0, 0, 0, 0 }),
      Unpooled.copiedBuffer(new byte[] { 0, 0, 0, 0, 0, 0, 0 })
  );

  private final IpcOption writeOption;
  private final FlightDescriptor descriptor;
  private final MessageMetadataResult message;
  private final ArrowBuf appMetadata;
  private final List<ArrowBuf> bufs;
  private final ArrowBodyCompression bodyCompression;

  public ArrowMessage(FlightDescriptor descriptor, Schema schema, IpcOption option) {
    this.writeOption = option;
    ByteBuffer serializedMessage = MessageSerializer.serializeMetadata(schema, writeOption);
    this.message = MessageMetadataResult.create(serializedMessage.slice(),
        serializedMessage.remaining());
    bufs = ImmutableList.of();
    this.descriptor = descriptor;
    this.appMetadata = null;
    this.bodyCompression = NoCompressionCodec.DEFAULT_BODY_COMPRESSION;
  }

  /**
   * Create an ArrowMessage from a record batch and app metadata.
   * @param batch The record batch.
   * @param appMetadata The app metadata. May be null. Takes ownership of the buffer otherwise.
   */
  public ArrowMessage(ArrowRecordBatch batch, ArrowBuf appMetadata, IpcOption option) {
    this.writeOption = option;
    ByteBuffer serializedMessage = MessageSerializer.serializeMetadata(batch, writeOption);
    this.message = MessageMetadataResult.create(serializedMessage.slice(), serializedMessage.remaining());
    this.bufs = ImmutableList.copyOf(batch.getBuffers());
    this.descriptor = null;
    this.appMetadata = appMetadata;
    this.bodyCompression = batch.getBodyCompression();
  }

  public ArrowMessage(ArrowDictionaryBatch batch, IpcOption option) {
    this.writeOption = new IpcOption();
    ByteBuffer serializedMessage = MessageSerializer.serializeMetadata(batch, writeOption);
    serializedMessage = serializedMessage.slice();
    this.message = MessageMetadataResult.create(serializedMessage, serializedMessage.remaining());
    // asInputStream will free the buffers implicitly, so increment the reference count
    batch.getDictionary().getBuffers().forEach(buf -> buf.getReferenceManager().retain());
    this.bufs = ImmutableList.copyOf(batch.getDictionary().getBuffers());
    this.descriptor = null;
    this.appMetadata = null;
    this.bodyCompression = batch.getDictionary().getBodyCompression();
  }

  /**
   * Create an ArrowMessage containing only application metadata.
   * @param appMetadata The application-provided metadata buffer.
   */
  public ArrowMessage(ArrowBuf appMetadata) {
    // No need to take IpcOption as it's not used to serialize this kind of message.
    this.writeOption = new IpcOption();
    this.message = null;
    this.bufs = ImmutableList.of();
    this.descriptor = null;
    this.appMetadata = appMetadata;
    this.bodyCompression = NoCompressionCodec.DEFAULT_BODY_COMPRESSION;
  }

  public ArrowMessage(FlightDescriptor descriptor) {
    // No need to take IpcOption as it's not used to serialize this kind of message.
    this.writeOption = new IpcOption();
    this.message = null;
    this.bufs = ImmutableList.of();
    this.descriptor = descriptor;
    this.appMetadata = null;
    this.bodyCompression = NoCompressionCodec.DEFAULT_BODY_COMPRESSION;
  }

  private ArrowMessage(FlightDescriptor descriptor, MessageMetadataResult message, ArrowBuf appMetadata,
                       ArrowBuf buf) {
    // No need to take IpcOption as this is used for deserialized ArrowMessage coming from the wire.
    this.writeOption = new IpcOption();
    if (message != null) {
      this.writeOption.metadataVersion = MetadataVersion.fromFlatbufID(message.getMessage().version());
    }
    this.message = message;
    this.descriptor = descriptor;
    this.appMetadata = appMetadata;
    this.bufs = buf == null ? ImmutableList.of() : ImmutableList.of(buf);
    this.bodyCompression = NoCompressionCodec.DEFAULT_BODY_COMPRESSION;
  }

  public MessageMetadataResult asSchemaMessage() {
    return message;
  }

  public FlightDescriptor getDescriptor() {
    return descriptor;
  }

  public HeaderType getMessageType() {
    if (message == null) {
      // Null message occurs for metadata-only messages (in DoExchange)
      return HeaderType.NONE;
    }
    return HeaderType.getHeader(message.headerType());
  }

  public Schema asSchema() {
    Preconditions.checkArgument(bufs.size() == 0);
    Preconditions.checkArgument(getMessageType() == HeaderType.SCHEMA);
    return MessageSerializer.deserializeSchema(message);
  }

  public ArrowRecordBatch asRecordBatch() throws IOException {
    Preconditions.checkArgument(bufs.size() == 1, "A batch can only be consumed if it contains a single ArrowBuf.");
    Preconditions.checkArgument(getMessageType() == HeaderType.RECORD_BATCH);

    ArrowBuf underlying = bufs.get(0);

    underlying.getReferenceManager().retain();
    return MessageSerializer.deserializeRecordBatch(message, underlying);
  }

  public ArrowDictionaryBatch asDictionaryBatch() throws IOException {
    Preconditions.checkArgument(bufs.size() == 1, "A batch can only be consumed if it contains a single ArrowBuf.");
    Preconditions.checkArgument(getMessageType() == HeaderType.DICTIONARY_BATCH);
    ArrowBuf underlying = bufs.get(0);
    // Retain a reference to keep the batch alive when the message is closed
    underlying.getReferenceManager().retain();
    // Do not set drained - we still want to release our reference
    return MessageSerializer.deserializeDictionaryBatch(message, underlying);
  }

  public Iterable<ArrowBuf> getBufs() {
    return Iterables.unmodifiableIterable(bufs);
  }

  private static ArrowMessage frame(BufferAllocator allocator, final InputStream stream) {

    try {
      FlightDescriptor descriptor = null;
      MessageMetadataResult header = null;
      ArrowBuf body = null;
      ArrowBuf appMetadata = null;
      while (stream.available() > 0) {
        int tag = readRawVarint32(stream);
        switch (tag) {

          case DESCRIPTOR_TAG: {
            int size = readRawVarint32(stream);
            byte[] bytes = new byte[size];
            ByteStreams.readFully(stream, bytes);
            descriptor = FlightDescriptor.parseFrom(bytes);
            break;
          }
          case HEADER_TAG: {
            int size = readRawVarint32(stream);
            byte[] bytes = new byte[size];
            ByteStreams.readFully(stream, bytes);
            header = MessageMetadataResult.create(ByteBuffer.wrap(bytes), size);
            break;
          }
          case APP_METADATA_TAG: {
            int size = readRawVarint32(stream);
            appMetadata = allocator.buffer(size);
            GetReadableBuffer.readIntoBuffer(stream, appMetadata, size, FAST_PATH);
            break;
          }
          case BODY_TAG:
            if (body != null) {
              // only read last body.
              body.getReferenceManager().release();
              body = null;
            }
            int size = readRawVarint32(stream);
            body = allocator.buffer(size);
            GetReadableBuffer.readIntoBuffer(stream, body, size, FAST_PATH);
            break;

          default:
            // ignore unknown fields.
        }
      }
      // Protobuf implementations can omit empty fields, such as body; for some message types, like RecordBatch,
      // this will fail later as we still expect an empty buffer. In those cases only, fill in an empty buffer here -
      // in other cases, like Schema, having an unexpected empty buffer will also cause failures.
      // We don't fill in defaults for fields like header, for which there is no reasonable default, or for appMetadata
      // or descriptor, which are intended to be empty in some cases.
      if (header != null) {
        switch (HeaderType.getHeader(header.headerType())) {
          case SCHEMA:
            // Ignore 0-length buffers in case a Protobuf implementation wrote it out
            if (body != null && body.capacity() == 0) {
              body.close();
              body = null;
            }
            break;
          case DICTIONARY_BATCH:
          case RECORD_BATCH:
            // A Protobuf implementation can skip 0-length bodies, so ensure we fill it in here
            if (body == null) {
              body = allocator.getEmpty();
            }
            break;
          case NONE:
          case TENSOR:
          default:
            // Do nothing
            break;
        }
      }
      return new ArrowMessage(descriptor, header, appMetadata, body);
    } catch (Exception ioe) {
      throw new RuntimeException(ioe);
    }

  }

  private static int readRawVarint32(InputStream is) throws IOException {
    int firstByte = is.read();
    return CodedInputStream.readRawVarint32(firstByte, is);
  }

  /**
   * Convert the ArrowMessage to an InputStream.
   *
   * <p>Implicitly, this transfers ownership of the contained buffers to the InputStream.
   *
   * @return InputStream
   */
  private InputStream asInputStream(BufferAllocator allocator) {
    if (message == null) {
      // If we have no IPC message, it's a pure-metadata message
      final FlightData.Builder builder = FlightData.newBuilder();
      if (descriptor != null) {
        builder.setFlightDescriptor(descriptor);
      }
      if (appMetadata != null) {
        builder.setAppMetadata(ByteString.copyFrom(appMetadata.nioBuffer()));
      }
      return NO_BODY_MARSHALLER.stream(builder.build());
    }

    try {
      final ByteString bytes = ByteString.copyFrom(message.getMessageBuffer(),
          message.bytesAfterMessage());

      if (getMessageType() == HeaderType.SCHEMA) {

        final FlightData.Builder builder = FlightData.newBuilder()
            .setDataHeader(bytes);

        if (descriptor != null) {
          builder.setFlightDescriptor(descriptor);
        }

        Preconditions.checkArgument(bufs.isEmpty());
        return NO_BODY_MARSHALLER.stream(builder.build());
      }

      Preconditions.checkArgument(getMessageType() == HeaderType.RECORD_BATCH ||
          getMessageType() == HeaderType.DICTIONARY_BATCH);
      // There may be no buffers in the case that we write only a null array
      Preconditions.checkArgument(descriptor == null, "Descriptor should only be included in the schema message.");

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      CodedOutputStream cos = CodedOutputStream.newInstance(baos);
      cos.writeBytes(FlightData.DATA_HEADER_FIELD_NUMBER, bytes);

      if (appMetadata != null && appMetadata.capacity() > 0) {
        // Must call slice() as CodedOutputStream#writeByteBuffer writes -capacity- bytes, not -limit- bytes
        cos.writeByteBuffer(FlightData.APP_METADATA_FIELD_NUMBER, appMetadata.nioBuffer().slice());
      }

      cos.writeTag(FlightData.DATA_BODY_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
      int size = 0;
      List<ByteBuf> allBufs = new ArrayList<>();
      for (ArrowBuf b : bufs) {
        allBufs.add(Unpooled.wrappedBuffer(b.nioBuffer()).retain());
        size += b.readableBytes();
        // [ARROW-4213] These buffers must be aligned to an 8-byte boundary in order to be readable from C++.
        if (b.readableBytes() % 8 != 0) {
          int paddingBytes = (int) (8 - (b.readableBytes() % 8));
          assert paddingBytes > 0 && paddingBytes < 8;
          size += paddingBytes;
          allBufs.add(PADDING_BUFFERS.get(paddingBytes).retain());
        }
      }
      // rawvarint is used for length definition.
      cos.writeUInt32NoTag(size);
      cos.flush();

      ByteBuf initialBuf = Unpooled.buffer(baos.size());
      initialBuf.writeBytes(baos.toByteArray());
      final CompositeByteBuf bb = new CompositeByteBuf(UnpooledByteBufAllocator.DEFAULT, true,
          Math.max(2, bufs.size() + 1),
          ImmutableList.<ByteBuf>builder()
              .add(initialBuf)
              .addAll(allBufs)
              .build());
      final ByteBufInputStream is = new DrainableByteBufInputStream(bb);
      return is;
    } catch (Exception ex) {
      throw new RuntimeException("Unexpected IO Exception", ex);
    }

  }

  private static class DrainableByteBufInputStream extends ByteBufInputStream implements Drainable {

    private final CompositeByteBuf buf;

    public DrainableByteBufInputStream(CompositeByteBuf buffer) {
      super(buffer, buffer.readableBytes(), true);
      this.buf = buffer;
    }

    @Override
    public int drainTo(OutputStream target) throws IOException {
      int size = buf.readableBytes();
      if (FAST_PATH && AddWritableBuffer.add(buf, target)) {
        return size;
      }

      buf.getBytes(0, target, buf.readableBytes());
      return size;
    }

    @Override
    public void close() {
      buf.release();
    }



  }

  public static Marshaller<ArrowMessage> createMarshaller(BufferAllocator allocator) {
    return new ArrowMessageHolderMarshaller(allocator);
  }

  private static class ArrowMessageHolderMarshaller implements Marshaller<ArrowMessage> {

    private final BufferAllocator allocator;

    public ArrowMessageHolderMarshaller(BufferAllocator allocator) {
      this.allocator = allocator;
    }

    @Override
    public InputStream stream(ArrowMessage value) {
      return value.asInputStream(allocator);
    }

    @Override
    public ArrowMessage parse(InputStream stream) {
      return ArrowMessage.frame(allocator, stream);
    }

  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(Iterables.concat(bufs, Collections.singletonList(appMetadata)));
  }
}
