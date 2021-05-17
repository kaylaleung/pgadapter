// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.spanner.pgadapter.wireprotocol;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.jdbc.CloudSpannerJdbcConnection;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 * Normally used to send data to the back-end. Spanner does not currently support this, so send will
 * throw a descriptive error to be sent to the user. Note that we do parse this in order for this to
 * be future proof, and to ensure the input stream is flushed of the command (in order to continue
 * receiving properly)
 */
public class CopyDataMessage extends ControlMessage {

  protected static final char IDENTIFIER = 'd';

  private byte[] payload;
  private IntermediateStatement statement;

  public CopyDataMessage(ConnectionHandler connection) throws Exception {
    super(connection);
    this.payload = new byte[this.length - 4];
    if (this.inputStream.read(this.payload) < 0) {
      throw new IOException("Could not read copy data.");
    }
    this.statement = connection.getActiveStatement();
  }

  @Override
  protected void sendPayload() throws Exception {
    String rowData = new String(this.payload, StandardCharsets.UTF_8).trim();
    writeToSpanner(rowData);
    this.statement.updateCount();
  }

  @Override
  protected String getMessageName() {
    return "Copy Data";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("Length: {0}").format(new Object[]{this.length});
  }

  @Override
  protected String getIdentifier() {
    return String.valueOf(IDENTIFIER);
  }

  public byte[] getPayload() {
    return this.payload;
  }

  public void writeToSpanner(String data)
      throws IOException, SQLException {
    Iterable<CSVRecord> records = CSVParser.parse(data, CSVFormat.POSTGRESQL_TEXT);

    // TODO: Clean this up for option flags
    // TODO: Support types specified by table columns
    List<Mutation> mutations = new ArrayList<>();
    Map<String, String> tableColumns = new LinkedHashMap<>();

    String tableName = "keyvalue"; // TODO: Replace this
    tableColumns.put("key", "placeholder"); // TODO: Replace this
    tableColumns.put("value", "placeholder"); // TODO: Replace this

    for (CSVRecord record : records) {
      int index = 0;
      WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(tableName);
      for (String columnName : tableColumns.keySet()) {
        String recordValue = record.get(index);
        index++;
        if (recordValue != null) {
          builder.set(columnName).to(recordValue);
        }
      }
      mutations.add(builder.build());
      Connection connection = this.connection.getJdbcConnection();
      CloudSpannerJdbcConnection spannerConnection = connection
          .unwrap(CloudSpannerJdbcConnection.class);
      spannerConnection.write(mutations);
    }
  }
}
