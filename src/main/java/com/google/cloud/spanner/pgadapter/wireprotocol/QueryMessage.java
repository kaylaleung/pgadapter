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

import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.ConnectionHandler.QueryMode;
import com.google.cloud.spanner.pgadapter.statements.CopyStatement;
import com.google.cloud.spanner.pgadapter.statements.IntermediateStatement;
import com.google.cloud.spanner.pgadapter.statements.PSQLStatement;
import com.google.cloud.spanner.pgadapter.wireoutput.CopyInResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.ReadyResponse;
import com.google.cloud.spanner.pgadapter.wireoutput.RowDescriptionResponse;
import java.text.MessageFormat;

/**
 * Executes a simple statement.
 */
public class QueryMessage extends ControlMessage {

  protected static final char IDENTIFIER = 'Q';
  protected static final String COPY = "COPY";

  private IntermediateStatement statement;
  private String sql;
  private String command;

  public QueryMessage(ConnectionHandler connection) throws Exception {
    super(connection);
    this.sql = this.readAll();
    this.command = parseCommand(sql);

    if (this.command.equals(COPY)) {
      this.statement = new CopyStatement(
          this.sql, this.connection.getJdbcConnection()
      );
    }
    else if (!connection.getServer().getOptions().isPSQLMode()) {
      this.statement = new IntermediateStatement(
          this.sql,
          this.connection.getJdbcConnection()
      );
    } else {
      this.statement = new PSQLStatement(
          this.sql,
          this.connection
      );
    }
    this.connection.addActiveStatement(this.statement);
  }

  @Override
  protected void sendPayload() throws Exception {
    this.statement.execute();
    this.handleQuery();
    if (!this.command.equals(COPY)) {
      this.connection.removeActiveStatement(this.statement);
    }
  }

  @Override
  protected String getMessageName() {
    return "Query";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat(
        "Length: {0}, SQL: {1}")
        .format(new Object[]{this.length, this.statement.getSql()});
  }

  @Override
  protected String getIdentifier() {
    return String.valueOf(IDENTIFIER);
  }

  public IntermediateStatement getStatement() {
    return this.statement;
  }

  /**
   * Simple Query handler, which examined the state of the statement and processes accordingly
   * (if error, handle error, otherwise sends the result and if contains result set,
   * send row description)
   *
   * @throws Exception
   */
  public void handleQuery() throws Exception {
    if (this.statement.hasException()) {
      this.handleError(this.statement.getException());
    } else {
      if (this.command.equals(COPY)) {
        CopyStatement copyStatement = (CopyStatement) this.statement;
        CopyInResponse copyIn = new CopyInResponse(this.outputStream,
            copyStatement,
            copyStatement.getColumnCount());
        copyIn.send();
      }
      else if (this.statement.containsResultSet()) {
        new RowDescriptionResponse(this.outputStream,
            this.statement,
            this.statement.getStatementResult().getMetaData(),
            this.connection.getServer().getOptions(),
            QueryMode.SIMPLE).send();
        this.sendSpannerResult(this.statement, QueryMode.SIMPLE, 0L);
        new ReadyResponse(this.outputStream, ReadyResponse.Status.IDLE).send();
      }
      else {
        this.sendSpannerResult(this.statement, QueryMode.SIMPLE, 0L);
        new ReadyResponse(this.outputStream, ReadyResponse.Status.IDLE).send();
      }
    }
    this.connection.cleanUp(this.statement);
  }

  private String parseCommand(String sql) {
    String[] tokens = sql.split("\\s+", 2);
    if (tokens.length > 0) {
      return tokens[0].toUpperCase();
    }
    return null;
  }
}
