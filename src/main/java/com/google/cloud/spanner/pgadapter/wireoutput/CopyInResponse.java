package com.google.cloud.spanner.pgadapter.wireoutput;

import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import java.io.DataOutputStream;
import java.io.IOException;

import java.text.MessageFormat;

public class CopyInResponse extends WireOutput {

  private static final int HEADER_LENGTH = 4;
  private static final int FORMAT_FLAG = 1;
  private static final int COLUMN_FORMAT_FLAG = 2;
  private static final int COLUMN_NUMBER = 2;

  protected static final char IDENTIFIER = 'G';

  private final IntermediateStatement statement;
  private final int columnCount;
  private final byte[] columnFormat;

  public CopyInResponse(DataOutputStream output,IntermediateStatement statement,
      int columnCount) throws Exception {
    super(output, calculateLength(columnCount));
    this.statement = statement;
    this.columnCount = columnCount;
    columnFormat = new byte[COLUMN_NUMBER * this.columnCount]; // Double to byte array size since should be a short array
  }

  private static int calculateLength(int columnCount) throws Exception {
    int length = HEADER_LENGTH + FORMAT_FLAG + COLUMN_NUMBER;
    length += COLUMN_FORMAT_FLAG * columnCount;
    // TODO: Adjust length of payload to send options info
    return length;
  }

  @Override
  protected void sendPayload() throws IOException {
    this.outputStream.writeByte(0); // COPY format is textual
    this.outputStream.writeShort(this.columnCount); // The number of columns in the data to be copied
    this.outputStream.write(this.columnFormat); // Format code 0 for all columns
    this.outputStream.flush();
  }

  @Override
  public byte getIdentifier() { return 'G'; }

  @Override
  protected String getMessageName() {
    return "Copy In";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat(
        "Length: {0}, "
            + "Columns Requested: {1}")
        .format(new Object[]{
            this.length,
            this.columnCount,
        });
  }
}
