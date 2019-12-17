package lama.sqlite3.gen.ddl;

import java.util.Arrays;

import lama.Query;
import lama.QueryAdapter;
import lama.Randomly;
import lama.sqlite3.SQLite3Provider.SQLite3GlobalState;

// see https://www.sqlite.org/lang_dropindex.html
public class SQLite3DropIndexGenerator {

	public static Query dropIndex(SQLite3GlobalState globalState) {
		String indexName = globalState.getSchema().getRandomIndexOrBailout();
		StringBuilder sb = new StringBuilder();
		sb.append("DROP INDEX ");
		if (Randomly.getBoolean()) {
			sb.append("IF EXISTS ");
		}
		sb.append('"');
		sb.append(indexName);
		sb.append('"');
		return new QueryAdapter(sb.toString(), Arrays.asList("[SQLITE_ERROR] SQL error or missing database (index associated with UNIQUE or PRIMARY KEY constraint cannot be dropped)"), true);
	}

}