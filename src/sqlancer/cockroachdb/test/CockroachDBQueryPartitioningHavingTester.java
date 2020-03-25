package sqlancer.cockroachdb.test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import sqlancer.DatabaseProvider;
import sqlancer.Randomly;
import sqlancer.TestOracle;
import sqlancer.cockroachdb.CockroachDBCommon;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider.CockroachDBGlobalState;
import sqlancer.cockroachdb.CockroachDBSchema;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBDataType;
import sqlancer.cockroachdb.CockroachDBSchema.CockroachDBTables;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.cockroachdb.ast.CockroachDBColumnReference;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBNotOperation;
import sqlancer.cockroachdb.ast.CockroachDBSelect;
import sqlancer.cockroachdb.ast.CockroachDBTableReference;
import sqlancer.cockroachdb.ast.CockroachDBUnaryPostfixOperation;
import sqlancer.cockroachdb.ast.CockroachDBUnaryPostfixOperation.CockroachDBUnaryPostfixOperator;
import sqlancer.cockroachdb.gen.CockroachDBExpressionGenerator;

public class CockroachDBQueryPartitioningHavingTester implements TestOracle {
	
	private final CockroachDBGlobalState state;
	private final Set<String> errors = new HashSet<>();

	public CockroachDBQueryPartitioningHavingTester(CockroachDBGlobalState state) {
		this.state = state;
		CockroachDBErrors.addExpressionErrors(errors);
		errors.add("GROUP BY term out of range");
	}

	@Override
	public void check() throws SQLException {
		CockroachDBSchema s = state.getSchema();
		CockroachDBTables targetTables = s.getRandomTableNonEmptyTables();
		CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(state).setColumns(targetTables.getColumns());
		CockroachDBSelect select = new CockroachDBSelect();
		select.setColumns(Arrays.asList(new CockroachDBColumnReference(targetTables.getColumns().get(0))));
		List<CockroachDBTableReference> tableList = targetTables.getTables().stream()
				.map(t -> new CockroachDBTableReference(t)).collect(Collectors.toList());
		List<CockroachDBTableReference> from = CockroachDBCommon.getTableReferences(tableList);
		if (Randomly.getBooleanWithRatherLowProbability()) {
			select.setJoinList(CockroachDBNoRECTester.getJoins(from, state));
		}
		select.setFromTables(from);
		// TODO order by?
		if (Randomly.getBoolean()) {
			select.setWhereCondition(gen.generateExpression(CockroachDBDataType.BOOL.get()));
		}
		select.setGroupByClause(gen.generateExpressions(Randomly.smallNumber() + 1));
		select.setHavingClause(null);
		String originalQueryString = CockroachDBVisitor.asString(select);
		if (state.getOptions().logEachSelect()) {
			state.getLogger().writeCurrent(originalQueryString);
			try {
				state.getLogger().getCurrentFileWriter().flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		List<String> resultSet = DatabaseProvider.getResultSetFirstColumnAsString(originalQueryString, errors, state.getConnection());
		
		CockroachDBExpression predicate = gen.generateHavingClause();
		select.setHavingClause(predicate);
		String firstQueryString = CockroachDBVisitor.asString(select);
		select.setHavingClause(new CockroachDBNotOperation(predicate));
		String secondQueryString = CockroachDBVisitor.asString(select);
		select.setHavingClause(new CockroachDBUnaryPostfixOperation(predicate, CockroachDBUnaryPostfixOperator.IS_NULL));
		String thirdQueryString = CockroachDBVisitor.asString(select);
		String combinedString = firstQueryString + " UNION ALL " + secondQueryString + " UNION ALL " + thirdQueryString;
		if (state.getOptions().logEachSelect()) {
			state.getLogger().writeCurrent(combinedString);
			try {
				state.getLogger().getCurrentFileWriter().flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		List<String> secondResultSet = DatabaseProvider.getResultSetFirstColumnAsString(combinedString, errors, state.getConnection());
		if (resultSet.size() != secondResultSet.size()) {
			throw new AssertionError(originalQueryString + ";\n" + combinedString + ";");
		}
	}
}