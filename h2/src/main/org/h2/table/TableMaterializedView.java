/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.h2.api.ErrorCode;
import org.h2.command.Prepared;
import org.h2.command.ddl.CreateTableData;
import org.h2.command.dml.AllColumnsForPlan;
import org.h2.command.dml.Query;
import org.h2.engine.Database;
import org.h2.engine.DbObject;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.expression.Alias;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionVisitor;
import org.h2.expression.Parameter;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.ViewIndex;
import org.h2.message.DbException;
import org.h2.result.ResultInterface;
import org.h2.result.Row;
import org.h2.result.SortOrder;
import org.h2.schema.Schema;
import org.h2.util.ColumnNamer;
import org.h2.util.StringUtils;
import org.h2.util.Utils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;

/**
 * A materialized view is a table in hard disk that is defined by a query.
 */
public class TableMaterializedView extends Table {

	private static final long ROW_COUNT_APPROXIMATION = 100;

	private String querySQL;
	private ArrayList<Table> tables;
	private Column[] columnTemplates;
	private Query viewQuery;
	private boolean allowRecursive;
	private DbException createException;
	private long lastModificationCheck;
	private long maxDataModificationId;
	private User owner;
	private Query topQuery;
	private boolean isRecursiveQueryDetected;
	private boolean isTableExpression;

	public TableMaterializedView(Schema schema, int id, String name, String querySQL,
			ArrayList<Parameter> params, Column[] columnTemplates, Session session,
			boolean allowRecursive, boolean literalsChecked, boolean isTableExpression, boolean isTemporary) {
		super(schema, id, name, false, true);
		setTemporary(isTemporary);
		init(querySQL, params, columnTemplates, session, allowRecursive, literalsChecked, isTableExpression);
	}

	private synchronized void init(String querySQL, ArrayList<Parameter> params,
			Column[] columnTemplates, Session session, boolean allowRecursive, boolean literalsChecked,
			boolean isTableExpression) {
		this.querySQL = querySQL;
		this.columnTemplates = columnTemplates;
		this.allowRecursive = allowRecursive;
		this.isRecursiveQueryDetected = false;
		this.isTableExpression = isTableExpression;
		initColumnsAndTables(session, literalsChecked);
	}

	private Query compileViewQuery(Session session, String sql, boolean literalsChecked, String viewName) {
		Prepared p;
		session.setParsingCreateView(true, viewName);
		try {
			p = session.prepare(sql, false, literalsChecked);
		} finally {
			session.setParsingCreateView(false, viewName);
		}
		if (!(p instanceof Query)) {
			throw DbException.getSyntaxError(sql, 0);
		}
		Query q = (Query) p;
		// only potentially recursive cte queries need to be non-lazy
		if (isTableExpression && allowRecursive) {
			q.setNeverLazy(true);
		}
		return q;
	}

	/**
	 * Re-compile the view query and all views that depend on this object.
	 *
	 * @param session the session
	 * @param force if exceptions should be ignored
	 * @param clearIndexCache if we need to clear view index cache
	 * @return the exception if re-compiling this or any dependent view failed
	 *         (only when force is disabled)
	 */
	public synchronized DbException recompile(Session session, boolean force,
			boolean clearIndexCache) {
		try {
			compileViewQuery(session, querySQL, false, getName());
		} catch (DbException e) {
			if (!force) {
				return e;
			}
		}
		ArrayList<TableMaterializedView> dependentViews = new ArrayList<>(getMaterializedViews());
		initColumnsAndTables(session, false);
		for (TableMaterializedView v : dependentViews) {
			DbException e = v.recompile(session, force, false);
			if (e != null && !force) {
				return e;
			}
		}
		if (clearIndexCache) {
			clearIndexCaches(database);
		}
		return force ? null : createException;
	}

	private void initColumnsAndTables(Session session, boolean literalsChecked) {
		Column[] cols;
		removeCurrentViewFromOtherTables();
		setTableExpression(isTableExpression);
		try {
			Query compiledQuery = compileViewQuery(session, querySQL, literalsChecked, getName());
			this.querySQL = compiledQuery.getPlanSQL(true);
			tables = new ArrayList<>(compiledQuery.getTables());
			ArrayList<Expression> expressions = compiledQuery.getExpressions();
			ColumnNamer columnNamer = new ColumnNamer(session);
			final int count = compiledQuery.getColumnCount();
			ArrayList<Column> list = new ArrayList<>(count);
			for (int i = 0; i < count; i++) {
				Expression expr = expressions.get(i);
				String name = null;
				TypeInfo type = TypeInfo.TYPE_UNKNOWN;
				if (columnTemplates != null && columnTemplates.length > i) {
					name = columnTemplates[i].getName();
					type = columnTemplates[i].getType();
				}
				if (name == null) {
					name = expr.getAlias();
				}
				name = columnNamer.getColumnName(expr, i, name);
				if (type.getValueType() == Value.UNKNOWN) {
					type = expr.getType();
				}
				Column col = new Column(name, type);
				col.setTable(this, i);
				// Fetch check constraint from view column source
				ExpressionColumn fromColumn = null;
				if (expr instanceof ExpressionColumn) {
					fromColumn = (ExpressionColumn) expr;
				} else if (expr instanceof Alias) {
					Expression aliasExpr = expr.getNonAliasExpression();
					if (aliasExpr instanceof ExpressionColumn) {
						fromColumn = (ExpressionColumn) aliasExpr;
					}
				}
				if (fromColumn != null) {
					Expression checkExpression = fromColumn.getColumn()
							.getCheckConstraint(session, name);
					if (checkExpression != null) {
						col.addCheckConstraint(session, checkExpression);
					}
				}
				list.add(col);
			}
			cols = list.toArray(new Column[0]);
			createException = null;
			viewQuery = compiledQuery;
		} catch (DbException e) {
			e.addSQL(getCreateSQL());
			createException = e;
			// If it can't be compiled, then it's a 'zero column table'
			// this avoids problems when creating the view when opening the
			// database.
			// If it can not be compiled - it could also be a recursive common
			// table expression query.
			if (isRecursiveQueryExceptionDetected(createException)) {
				this.isRecursiveQueryDetected = true;
			}
			tables = Utils.newSmallArrayList();
			cols = new Column[0];
			if (allowRecursive && columnTemplates != null) {
				cols = new Column[columnTemplates.length];
				for (int i = 0; i < columnTemplates.length; i++) {
					cols[i] = columnTemplates[i].getClone();
				}
				createException = null;
			}
		}
		setColumns(cols);
		if (getId() != 0) {
			addDependentViewToTables();
		}
	}

	@Override
	public boolean isView() {
		return true;
	}

	/**
	 * Check if this view is currently invalid.
	 *
	 * @return true if it is
	 */
	public boolean isInvalid() {
		return createException != null;
	}

	@Override
	public boolean isQueryComparable() {
		if (!super.isQueryComparable()) {
			return false;
		}
		for (Table t : tables) {
			if (!t.isQueryComparable()) {
				return false;
			}
		}
		if (topQuery != null &&
				!topQuery.isEverything(ExpressionVisitor.QUERY_COMPARABLE_VISITOR)) {
			return false;
		}
		return true;
	}

	public Query getTopQuery() {
		return topQuery;
	}

	@Override
	public String getDropSQL() {
		return "DROP MATERIALIZED VIEW IF EXISTS " + getSQL(true) + " CASCADE";
	}

	@Override
	public String getCreateSQLForCopy(Table table, String quotedName) {
		return getCreateSQL(false, true, quotedName);
	}


	@Override
	public String getCreateSQL() {
		return getCreateSQL(false, true);
	}

	/**
	 * Generate "CREATE" SQL statement for the view.
	 *
	 * @param orReplace if true, then include the OR REPLACE clause
	 * @param force if true, then include the FORCE clause
	 * @return the SQL statement
	 */
	public String getCreateSQL(boolean orReplace, boolean force) {
		return getCreateSQL(orReplace, force, getSQL(true));
	}

	private String getCreateSQL(boolean orReplace, boolean force, String quotedName) {
		StringBuilder builder = new StringBuilder("CREATE ");
		if (orReplace) {
			builder.append("OR REPLACE ");
		}
		if (force) {
			builder.append("FORCE ");
		}
		builder.append("MATERIALIZED VIEW ");
		if (isTableExpression) {
			builder.append("TABLE_EXPRESSION ");
		}
		builder.append(quotedName);
		if (comment != null) {
			builder.append(" COMMENT ");
			StringUtils.quoteStringSQL(builder, comment);
		}
		if (columns != null && columns.length > 0) {
			builder.append('(');
			Column.writeColumns(builder, columns, true);
			builder.append(')');
		} else if (columnTemplates != null) {
			builder.append('(');
			Column.writeColumns(builder, columnTemplates, true);
			builder.append(')');
		}
		return builder.append(" AS\n").append(querySQL).toString();
	}

	@Override
	public void checkRename() {
		// ok
	}

	@Override
	public boolean lock(Session session, boolean exclusive, boolean forceLockEvenInMvcc) {
		// exclusive lock means: the view will be dropped
		return false;
	}

	@Override
	public void close(Session session) {
		// nothing to do
	}

	@Override
	public void unlock(Session s) {
		// nothing to do
	}

	@Override
	public boolean isLockedExclusively() {
		return false;
	}

	@Override
	public Index addIndex(Session session, String indexName, int indexId,
			IndexColumn[] cols, IndexType indexType, boolean create,
			String indexComment) {
		throw DbException.getUnsupportedException("VIEW");
	}

	@Override
	public void removeRow(Session session, Row row) {
		throw DbException.getUnsupportedException("VIEW");
	}

	@Override
	public void addRow(Session session, Row row) {
		throw DbException.getUnsupportedException("VIEW");
	}

	@Override
	public void checkSupportAlter() {
		throw DbException.getUnsupportedException("VIEW");
	}

	@Override
	public void truncate(Session session) {
		throw DbException.getUnsupportedException("VIEW");
	}

	@Override
	public long getRowCount(Session session) {
		throw DbException.throwInternalError(toString());
	}

	@Override
	public boolean canGetRowCount() {
		// TODO view: could get the row count, but not that easy
		return false;
	}

	@Override
	public boolean canDrop() {
		return true;
	}

	@Override
	public TableType getTableType() {
		return TableType.VIEW;
	}

	@Override
	public void removeChildrenAndResources(Session session) {
		removeCurrentViewFromOtherTables();
		super.removeChildrenAndResources(session);
		database.removeMeta(session, getId());
		querySQL = null;
		clearIndexCaches(database);
		invalidate();
	}

	/**
	 * Clear the cached indexes for all sessions.
	 *
	 * @param database the database
	 */
	public static void clearIndexCaches(Database database) {
		for (Session s : database.getSessions(true)) {
			s.clearViewIndexCache();
		}
	}

	@Override
	public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
		if (isTemporary() && querySQL != null) {
			builder.append("(\n");
			return StringUtils.indent(builder, querySQL, 4, true).append(')');
		}
		return super.getSQL(builder, alwaysQuote);
	}

	// KEEP! Important for refresh query
	public String getQuery() {
		return querySQL;
	}

	@Override
	public Index getScanIndex(Session session) {
		return null;
	}

	@Override
	public Index getScanIndex(Session session, int[] masks,
			TableFilter[] filters, int filter, SortOrder sortOrder,
			AllColumnsForPlan allColumnsSet) {
		if (createException != null) {
			String msg = createException.getMessage();
			throw DbException.get(ErrorCode.VIEW_IS_INVALID_2,
					createException, getSQL(false), msg);
		}
		PlanItem item = getBestPlanItem(session, masks, filters, filter, sortOrder, allColumnsSet);
		return item.getIndex();
	}

	@Override
	public boolean canReference() {
		return false;
	}

	@Override
	public ArrayList<Index> getIndexes() {
		return null;
	}

	@Override
	public long getMaxDataModificationId() {
		if (createException != null) {
			return Long.MAX_VALUE;
		}
		if (viewQuery == null) {
			return Long.MAX_VALUE;
		}
		// if nothing was modified in the database since the last check, and the
		// last is known, then we don't need to check again
		// this speeds up nested views
		long dbMod = database.getModificationDataId();
		if (dbMod > lastModificationCheck && maxDataModificationId <= dbMod) {
			maxDataModificationId = viewQuery.getMaxDataModificationId();
			lastModificationCheck = dbMod;
		}
		return maxDataModificationId;
	}

	@Override
	public Index getUniqueIndex() {
		return null;
	}

	private void removeCurrentViewFromOtherTables() {
		if (tables != null) {
			for (Table t : tables) {
				t.removeDependentView(this);
			}
			tables.clear();
		}
	}

	private void addDependentViewToTables() {
		for (Table t : tables) {
			t.addDependentView(this);
		}
	}

	private void setOwner(User owner) {
		this.owner = owner;
	}

	public User getOwner() {
		return owner;
	}

	/**
	 * Create a temporary view out of the given query.
	 *
	 * @param session the session
	 * @param owner the owner of the query
	 * @param name the view name
	 * @param query the query
	 * @param topQuery the top level query
	 * @return the view table
	 */
	public static TableMaterializedView createTempView(Session session, User owner,
			String name, Query query, Query topQuery) {
		Schema mainSchema = session.getDatabase().getMainSchema();
		String querySQL = query.getPlanSQL(true);
		TableMaterializedView v = new TableMaterializedView(mainSchema, 0, name,
				querySQL, query.getParameters(), null /* column templates */, session,
				false/* allow recursive */, true /* literals have already been checked when parsing original query */,
				false /* is table expression */, true/*temporary*/);
		if (v.createException != null) {
			throw v.createException;
		}
		v.setTopQuery(topQuery);
		v.setOwner(owner);
		v.setTemporary(true);
		return v;
	}

	private void setTopQuery(Query topQuery) {
		this.topQuery = topQuery;
	}

	@Override
	public long getRowCountApproximation() {
		return ROW_COUNT_APPROXIMATION;
	}

	@Override
	// CSC-468 TO DO: Change this because materialized view should use up disk space.
	public long getDiskSpaceUsed() {
		return 0;
	}

	/**
	 * Get the index of the first parameter.
	 *
	 * @param additionalParameters additional parameters
	 * @return the index of the first parameter
	 */
	public int getParameterOffset(ArrayList<Parameter> additionalParameters) {
		int result = topQuery == null ? -1 : getMaxParameterIndex(topQuery.getParameters());
		if (additionalParameters != null) {
			result = Math.max(result, getMaxParameterIndex(additionalParameters));
		}
		return result + 1;
	}

	private static int getMaxParameterIndex(ArrayList<Parameter> parameters) {
		int result = -1;
		for (Parameter p : parameters) {
			result = Math.max(result, p.getIndex());
		}
		return result;
	}

	public boolean isRecursive() {
		return allowRecursive;
	}

	@Override
	public boolean isDeterministic() {
		if (allowRecursive || viewQuery == null) {
			return false;
		}
		return viewQuery.isEverything(ExpressionVisitor.DETERMINISTIC_VISITOR);
	}

	@Override
	public void addDependencies(HashSet<DbObject> dependencies) {
		super.addDependencies(dependencies);
		if (tables != null) {
			for (Table t : tables) {
				if (TableType.VIEW != t.getTableType()) {
					t.addDependencies(dependencies);
				}
			}
		}
	}

	/**
	 * Was query recursion detected during compiling.
	 *
	 * @return true if yes
	 */
	public boolean isRecursiveQueryDetected() {
		return isRecursiveQueryDetected;
	}

	/**
	 * Does exception indicate query recursion?
	 */
	private boolean isRecursiveQueryExceptionDetected(DbException exception) {
		if (exception == null) {
			return false;
		}
		if (exception.getErrorCode() != ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1) {
			return false;
		}
		return exception.getMessage().contains("\"" + this.getName() + "\"");
	}

	public List<Table> getTables() {
		return tables;
	}
}