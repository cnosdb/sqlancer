package postgres.gen;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.postgresql.util.PSQLException;

import lama.Main;
import lama.Main.QueryManager;
import lama.Main.StateLogger;
import lama.MainOptions;
import lama.Randomly;
import lama.StateToReproduce.PostgresStateToReproduce;
import postgres.PostgresSchema;
import postgres.PostgresSchema.PostgresColumn;
import postgres.PostgresSchema.PostgresRowValue;
import postgres.PostgresSchema.PostgresTables;
import postgres.PostgresToStringVisitor;
import postgres.ast.PostgresColumnValue;
import postgres.ast.PostgresConstant;
import postgres.ast.PostgresExpression;
import postgres.ast.PostgresOrderByTerm;
import postgres.ast.PostgresOrderByTerm.PostgresOrder;
import postgres.ast.PostgresSelect;

public class PostgresQueryGenerator {

	private Randomly r;
	private PostgresStateToReproduce state;
	private PostgresRowValue rw;
	private Connection database;
	private List<PostgresColumn> fetchColumns;
	private PostgresSchema s;

	public PostgresQueryGenerator(QueryManager manager, Randomly r, Connection con, String databaseName)
			throws SQLException {
		this.r = r;
		this.database = con;
		this.s = PostgresSchema.fromConnection(con, databaseName);
	}

	public void generateAndCheckQuery(PostgresStateToReproduce state, StateLogger logger, MainOptions options)
			throws SQLException {

		String queryString = getQueryThatContainsAtLeastOneRow(state);
		state.queryString = queryString;
		if (options.logEachSelect()) {
			logger.writeCurrent(state.queryString);
		}

		boolean isContainedIn = isContainedIn(queryString, options, logger);
		if (!isContainedIn) {
			throw new Main.ReduceMeException();
		}

	}

	public String getQueryThatContainsAtLeastOneRow(PostgresStateToReproduce state) throws SQLException {
		this.state = state;
		PostgresTables randomFromTables = s.getRandomTableNonEmptyTables();

		state.queryTargetedTablesString = randomFromTables.tableNamesAsString();

		PostgresSelect selectStatement = new PostgresSelect();
		selectStatement.setSelectType(Randomly.fromOptions(PostgresSelect.SelectType.values()));
		List<PostgresColumn> columns = randomFromTables.getColumns();
		rw = randomFromTables.getRandomRowValue(database, state);

		fetchColumns = columns;
		selectStatement.setFromList(randomFromTables.getTables());
		selectStatement.selectFetchColumns(fetchColumns);
		state.queryTargetedColumnsString = fetchColumns.stream().map(c -> c.getFullQualifiedName())
				.collect(Collectors.joining(", "));
		PostgresExpression whereClause = generateWhereClauseThatContainsRowValue(columns, rw);
		selectStatement.setWhereClause(whereClause);
		state.whereClause = selectStatement;
		List<PostgresExpression> groupByClause = generateGroupByClause(columns, rw);
		selectStatement.setGroupByClause(groupByClause);
		PostgresExpression limitClause = generateLimit();
		selectStatement.setLimitClause(limitClause);
		if (limitClause != null) {
			PostgresExpression offsetClause = generateOffset();
			selectStatement.setOffsetClause(offsetClause);
		}
		List<String> modifiers = Collections.emptyList(); // "SQL_BUFFER_RESULT",
															// "SQL_CALC_FOUND_ROWS",
															// "HIGH_PRIORITY"
		selectStatement.setModifiers(modifiers);
		List<PostgresExpression> orderBy = generateOrderBy(columns);
		selectStatement.setOrderByClause(orderBy);

		StringBuilder sb2 = new StringBuilder();
		sb2.append("SELECT * FROM (SELECT 1 FROM ");
		sb2.append(randomFromTables.tableNamesAsString());
		sb2.append(" WHERE ");
		int i = 0;
		for (PostgresColumn c : fetchColumns) {
			if (i++ != 0) {
				sb2.append(" AND ");
			}
			sb2.append(c.getFullQualifiedName());
			if (rw.getValues().get(c).isNull()) {
				sb2.append(" IS NULL");
			} else {
				sb2.append(" = ");
				sb2.append(rw.getValues().get(c).getTextRepresentation());
			}
		}
		sb2.append(") as result;");
		state.queryThatSelectsRow = sb2.toString();

		PostgresToStringVisitor visitor = new PostgresToStringVisitor();
		visitor.visit(selectStatement);
		String queryString = visitor.get();
		return queryString;
	}

	private List<PostgresExpression> generateGroupByClause(List<PostgresColumn> columns, PostgresRowValue rw) {
		if (Randomly.getBoolean()) {
			return columns.stream().map(c -> PostgresColumnValue.create(c, rw.getValues().get(c)))
					.collect(Collectors.toList());
		} else {
			return Collections.emptyList();
		}
	}

	public List<PostgresExpression> generateOrderBy(List<PostgresColumn> columns) {
		List<PostgresExpression> orderBys = new ArrayList<>();
		for (int i = 0; i < Randomly.smallNumber(); i++) {
			orderBys.add(new PostgresOrderByTerm(PostgresColumnValue.create(Randomly.fromList(columns), null),
					PostgresOrder.getRandomOrder()));
		}
		return orderBys;
	}

	private PostgresConstant generateLimit() {
		if (Randomly.getBoolean()) {
			return PostgresConstant.createIntConstant(Integer.MAX_VALUE);
		} else {
			return null;
		}
	}

	private PostgresExpression generateOffset() {
		if (Randomly.getBoolean()) {
			// OFFSET 0
			return PostgresConstant.createIntConstant(0);
		} else {
			return null;
		}
	}

	private PostgresExpression generateWhereClauseThatContainsRowValue(List<PostgresColumn> columns,
			PostgresRowValue rw) {
		PostgresExpression expression = PostgresExpressionGenerator.generateTrueCondition(columns, rw, r);
		return expression;
	}

	private boolean isContainedIn(String queryString, MainOptions options, StateLogger logger) throws SQLException {
		Statement createStatement;
		createStatement = database.createStatement();

		StringBuilder sb = new StringBuilder();
		sb.append("SELECT * FROM ("); // ANOTHER SELECT TO USE ORDER BY without restrictions
		sb.append(queryString);
		sb.append(") as result WHERE ");
		int i = 0;
		for (PostgresColumn c : fetchColumns) {
			if (i++ != 0) {
				sb.append(" AND ");
			}
			sb.append("result." + c.getTable().getName() + c.getName());
			if (rw.getValues().get(c).isNull()) {
				sb.append(" IS NULL");
			} else {
				sb.append(" = ");
				sb.append(rw.getValues().get(c).getTextRepresentation());
			}
		}
		String resultingQueryString = sb.toString();
		state.queryString = resultingQueryString;
		if (options.logEachSelect()) {
			logger.writeCurrent(resultingQueryString);
		}
		try (ResultSet result = createStatement.executeQuery(resultingQueryString)) {
			boolean isContainedIn = result.next();
			createStatement.close();
			return isContainedIn;
		} catch (PSQLException e) {
			if (e.getMessage().contains("out of range") || e.getMessage().contains("cannot cast")
					|| e.getMessage().contains("invalid input syntax for ") || e.getMessage().contains("must be type")
					|| e.getMessage().contains("operator does not exist")
					|| e.getMessage().contains("Could not choose a best candidate function.")
					|| e.getMessage().contains("division by zero")
					|| e.getMessage().contains("zero raised to a negative power is undefined")
					|| e.getMessage().contains("canceling statement due to statement timeout")) {
				return true;
			} else {
				throw e;
			}
		}
	}

}
