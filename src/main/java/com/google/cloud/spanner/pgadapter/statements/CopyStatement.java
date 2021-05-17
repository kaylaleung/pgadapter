package com.google.cloud.spanner.pgadapter.statements;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CopyStatement extends IntermediateStatement {
  private int columnCount = 0;

  public CopyStatement(String sql, Connection connection) throws SQLException {
    super();
    this.sql = sql;
    this.command = parseCommand(sql);
    this.connection = connection;
    this.statement = connection.createStatement();
  }

  private void parseCopy(String sql) throws SQLException {
    // TODO: Insert parse COPY statement for table name
    String tablename = "keyvalue"; // TODO: Hard Coded
    ResultSet spannerType = this.connection.createStatement()
        .executeQuery("SELECT column_name FROM information_schema.columns WHERE table_name = \""
            + tablename + "\" ORDER BY ordinal_position");
    while (spannerType.next()) {
      this.columnCount++;
    }
    // TODO: Parse COPY statement for option flags
  }

  public int getColumnCount() {
    return columnCount;
  }

  @Override
  public void execute() {
    this.executed = true; // Maybe this shouldn't be set to true until after the CopyData?
    try {
      parseCopy(this.sql);
      this.executeHelper();
    } catch (SQLException e) {
      handleExecutionException(e);
    }
  }

  protected void executeHelper() throws SQLException {
    this.resultType = ResultType.UPDATE_COUNT;
    this.updateCount = this.statement.getUpdateCount();
    this.hasMoreData = false;
    this.statementResult = null;
  }
}
