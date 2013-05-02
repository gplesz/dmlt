using System;
using System.Text;
using Microsoft.SqlServer.Server;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Data;

namespace dmlt
{
    public class CLRTriggers
    {
        private const string mcstrParam_Server1 = "SERVER1";
        private const string mcstrParam_Server2 = "SERVER2";
        private const string mcstrParam_Database1 = "DATABASE1";

        private const string mcstrParam_Database2 = "DATABASE2";
        //Private Const mcstrWinMerge As String = "VDiffCmdLine.exe"
        //Private Const mcstrParam_Tables As String = "TABLES"
        //Private Const mcstrDiffHtml As String = "diff.html"

        private const string mcstrMcExp_select = "select ";

        private const string mcstrMcExp_where = "where ";
        //Private Const mcstrMcExp_Table1 As String = " from [{db1}].[dbo].[{tbl}] t1 "
        //Private Const mcstrMcExp_Table1 As String = " from [{0}].[{1}].[{2}] t1 "
        //Private Const mcstrMcExp_Table2 As String = "full outer join [{db2}].[dbo].[{tbl}] t2 on "

        private const string mcstrMcExp_Join = "([t1].[{key}]=[t2].[{key}]) and ";
        //Private Const mcstrMcExp_Join As String = "([t1].[{0}]=[t2].[{0}]) and "
        private const string mcstrMcExp_WhereIns = "([t2].[{key}] is null ) and ";
        private const string mcstrMcExp_WhereDel = "([t1].[{key}] is null ) and ";

        private const string mcstrMcExp_WhereUpd = "([t1].[{column}]<>[t2].[{column}]) or ";
        private const string mcstrMcExp_ListDel = "[t2].[{key}], ";
        private const string mcstrMcExp_ListIns = "[t1].[{column}], ";

        private const string mcstrMcExp_ListUpd = "[t1].[{column}] as [t1.{column}], [t2].[{column}] as [t2.{column}], ";
        private const string mcstrMcExp_ListInsFlds = "[{column}], ";
        private const string mcstrMcExp_ListInsVls = "{value}, ";
        //Private Const mcstrMcExp_ListUpdFldsVls As String = "[{column}]={value}"

        private const string mcstrMcFld_key = "{key}";
        private const string mcstrMcFld_column = "{column}";

        private const string mcstrMcFld_value = "{value}";
        private const string mcstrMcFld_tbl = "{tbl}";
        private const string mcstrMcFld_db1 = "{db1}";

        private const string mcstrMcFld_db2 = "{db2}";

        private const string mcstrMcExp_constraint = "{constraint}";
        private const string mcstrMcExp_BgnTran = "begin transaction";

        private const string mcstrMcExp_ComTran = "commit transaction";

        [SqlTrigger(Name = "trgTable_Modify", Event = "FOR INSERT, UPDATE, DELETE")]
        public static void trgTable_Modify()
        {
	        SqlCommand command = default(SqlCommand);
	        SqlTriggerContext triggContext = default(SqlTriggerContext);
	        SqlPipe pipe = default(SqlPipe);

	        //Szövegek tárolói
	        int lngCnt1 = 0;
	        long lngX = 0;

	        string strJoin = null;
	        string strColumn = null;
	        string strWhereDel = null;
	        string strWhereIns = null;
	        string strWhereUpd = null;
	        string strListDel = null;
	        string strListIns = null;
	        string strListUpd = null;

	        string strQueryDel = null;
	        string strQueryIns = null;
	        string strQueryUpd = null;

	        string strQuery = null;

	        string strDelRun = null;
	        string strDelRunWhere = null;
	        string strInsRun = null;
	        string strInsRunFields = null;
	        string strInsRunValues = null;
	        string strUpdRun = null;
	        string strUpdRunList = null;
	        string strUpdRunWhere = null;

	        string strSchemaName = null;
	        string strTableName = null;
	        long lngZ = 0;
	        string strFldName = null;

	        string strFkOut = null;
	        string strFkIn = null;
	        string strIdOff = null;
	        string strIdOn = null;

	        string strCommand = null;
	        //A módosított tábla oszlopainak száma
	        long lngCols = 0;
	        //A lekérdezésekhez tableloader
	        SqlDataAdapter tblLoader = default(SqlDataAdapter);
	        //Az alaptábla struktúrájának meghatározásához
	        DataTable tblTableStruct = default(DataTable);
	        //Kapcsolat az adatbázissal
	        SqlConnection dbConnection = default(SqlConnection);
	        //Oszloptömb a kulcsok beolvasásához
	        DataColumn[] colPK = null;
	        //FK-k beolvasásához
	        DataTable tblFK = default(DataTable);
	        //a mezők beolvasásához
	        DataColumnCollection colFields = default(DataColumnCollection);
	        DataColumn col = default(DataColumn);

	        //A megváltozott adatokat ide töltjük
	        DataTable tblModifyData = default(DataTable);
	        long lngRowsCnt = 0;
	        DataRow rowCurrent = default(DataRow);
	        StringBuilder sb = default(StringBuilder);

	        long lngRowCount = 0;

	        strFkOut = string.Empty;
	        strFkIn = string.Empty;

	        strIdOn = string.Empty;
	        strIdOff = string.Empty;


        #if DEBUG
            Common.fpDebugMessage("entering into trgTable_Modify", TraceEventType.Start);
        #endif

        #if DEBUG
            Common.fpDebugMessage("context information", TraceEventType.Verbose);
        #endif

	        triggContext = SqlContext.TriggerContext;
	        pipe = SqlContext.Pipe;

	        strCommand = string.Empty;

        #if DEBUG
            Common.fpDebugMessage("open connection", TraceEventType.Verbose);
        #endif

	        dbConnection = new SqlConnection("context connection=true");
	        dbConnection.Open();

        #if DEBUG
            Common.fpDebugMessage("table name identification", TraceEventType.Verbose);
        #endif

	        command = new SqlCommand();

	        sb = new StringBuilder();
	        sb.Append("select object_schema_name(resource_associated_entity_id) strSchemaName");
	        sb.Append(",object_name(resource_associated_entity_id) strTableName ");
	        sb.Append(",object_id(object_schema_name(resource_associated_entity_id) + '.' + object_name(resource_associated_entity_id), 'U') object_id ");
	        //sb.Append(",* ")
	        //sb.Append(",(select text from sys.dm_exec_sql_text(sql_handle)) As strLastSQLText ")
	        sb.Append("from ");
	        sb.Append("sys.dm_tran_locks ");
	        //sb.Append("inner join master.sys.sysprocesses on spid=request_session_id ")
	        sb.Append(" where ");
	        sb.Append("(request_session_id = @@spid)");
	        sb.Append(" and ");
	        sb.Append("(resource_type = 'OBJECT')");

	        //fpDebugMessage("qry: " & sb.ToString, TraceEventType.Information)

	        command.CommandText = sb.ToString();
	        command.CommandType = CommandType.Text;
	        command.Connection = dbConnection;

	        SqlDataAdapter da = new SqlDataAdapter(command);
	        DataSet rdData = new DataSet();
	        lngRowsCnt = da.Fill(rdData);


        #if DEBUG
            Common.fpDebugMessage("Rows: " + lngRowsCnt.ToString(), TraceEventType.Information);

	        tblLoader = new SqlDataAdapter(sb.ToString(), dbConnection);

	        tblLoader.SelectCommand.CommandText = "select top 1 * from inserted";
	        tblModifyData = new DataTable();
	        tblLoader.Fill(tblModifyData);

	        bool ysnFoundTable = false;

	        foreach (DataRow r in rdData.Tables[0].Rows) {
		        strSchemaName = r["strSchemaName"].ToString();
		        strTableName = r["strTableName"].ToString();


		        if (null!=r["object_id"]) {

			        if ("dbo#schSac#schSdb#schStr".Contains((string)r["strSchemaName"])) {
				        sb = new StringBuilder();
				        sb.AppendFormat("select top 1 * from [{0}].[{1}]", strSchemaName, strTableName);

				        tblLoader.SelectCommand.CommandText = sb.ToString();
				        DataTable tbl = new DataTable();
				        tblLoader.Fill(tbl);
				        if (tbl.Columns[0].ColumnName.Equals(tblModifyData.Columns[0].ColumnName)) {
					        //Megvan a táblánk
					        Common.fpDebugMessage("found table: " + strSchemaName + "." + strTableName, TraceEventType.Verbose);
					        ysnFoundTable = true;
					        break; // TODO: might not be correct. Was : Exit For
				        } else {
					        Common.fpDebugMessage("table not pass: " + strSchemaName + "." + strTableName, TraceEventType.Verbose);
					        //nincs meg a táblánk
				        }
			        } else {
				        Common.fpDebugMessage("table not pass (sys): " + strSchemaName + "." + strTableName, TraceEventType.Verbose);
			        }
		        } else {
			        Common.fpDebugMessage("table not pass (object_id): " + strSchemaName + "." + strTableName, TraceEventType.Verbose);
		        }
	        }

	        if (false == ysnFoundTable) {
		        throw new ArgumentNullException("Nem sikerült beazonosítani a forrás táblát");
	        }

        #endif

        #if DEBUG
	        Common.fpDebugMessage("load table struct: " + strSchemaName + "." + strTableName, TraceEventType.Verbose);
        #endif


	        sb = new StringBuilder();
	        sb.AppendFormat("select top 1 * from [{0}].[{1}]", strSchemaName, strTableName);

	        tblLoader = new SqlDataAdapter(sb.ToString(), dbConnection);
	        //tblLoader.SelectCommand.CommandText = "select top 1 * from " & strTableName

	        tblTableStruct = new DataTable();
	        tblLoader.FillSchema(tblTableStruct, SchemaType.Mapped);

	        colFields = tblTableStruct.Columns;
	        colPK = tblTableStruct.PrimaryKey;

        #if DEBUG
	        Common.fpDebugMessage("identify primary key(s)", TraceEventType.Information);
        #endif

	        strJoin = string.Empty;
	        strWhereDel = mcstrMcExp_where;
	        strWhereIns = mcstrMcExp_where;
	        strWhereUpd = mcstrMcExp_where;
	        strListDel = mcstrMcExp_select;
	        strListIns = mcstrMcExp_select;
	        strListUpd = mcstrMcExp_select;

	        lngCols = colPK.GetUpperBound(0);

	        if (lngCols >= 0) {
		        for (lngX = 0; lngX <= lngCols; lngX++) {
			        Common.fpDebugMessage(colPK[lngX].ColumnName + " Key(" + lngX.ToString() + "): " + colPK[lngX].DataType.ToString() + ", Identity: " + colPK[lngX].AutoIncrement.ToString(), TraceEventType.Information);
			        strColumn = colPK[lngX].ColumnName;

			        strJoin = strJoin + mcstrMcExp_Join.Replace(mcstrMcFld_key, strColumn);


			        if (colPK[lngX].AutoIncrement == true) {
				        sb = new StringBuilder();
				        sb.AppendFormat("set identity_insert [{0}].[{1}] OFF", strSchemaName, strTableName);
				        strIdOff = strIdOff + sb.ToString();

				        sb = new StringBuilder();
				        sb.AppendFormat("set identity_insert [{0}].[{1}] ON", strSchemaName, strTableName);
				        strIdOn = strIdOn + sb.ToString();

			        }
			        strWhereDel = strWhereDel + mcstrMcExp_WhereDel.Replace(mcstrMcFld_key, strColumn);
			        strWhereIns = strWhereIns + mcstrMcExp_WhereIns.Replace(mcstrMcFld_key, strColumn);
			        strListDel = strListDel + mcstrMcExp_ListDel.Replace(mcstrMcFld_key, strColumn);

		        }
	        } else {
		        Common.fpDebugMessage("primary key not available", TraceEventType.Error);
		        throw new System.Exception("primary key not found in the table: " + strTableName);
	        }


	        Common.fpDebugMessage("Fields:", TraceEventType.Verbose);
	        foreach (DataColumn c in colFields) {
		        //ha nem ntext-ről van szó
		        if (c.MaxLength != 1073741823) {
			        Common.fpDebugMessage(c.ColumnName.ToString() + "(" + c.DataType.ToString() + ")" + "(" + c.MaxLength.ToString() + ")", TraceEventType.Verbose);
			        //If col.DataType IsNot System.Type.GetType("") Then
			        //ha nem text mező
			        //End If
			        strColumn = c.ColumnName.ToString();
			        strListIns = strListIns + mcstrMcExp_ListIns.Replace(mcstrMcFld_column, strColumn);
			        strWhereUpd = strWhereUpd + mcstrMcExp_WhereUpd.Replace(mcstrMcFld_column, strColumn);
			        strListUpd = strListUpd + mcstrMcExp_ListUpd.Replace(mcstrMcFld_column, strColumn);
		        }
	        }

	        strJoin = strJoin.Substring(0, strJoin.Length - 4);

	        strWhereUpd = strWhereUpd.Substring(0, strWhereUpd.Length - 4);
	        strListUpd = strListUpd.Substring(0, strListUpd.Length - 2);
	        strListIns = strListIns.Substring(0, strListIns.Length - 2);
	        strListDel = strListDel.Substring(0, strListDel.Length - 2);

	        Common.fpDebugMessage("join on primary keys:", TraceEventType.Verbose);
	        Common.fpDebugMessage(strJoin, TraceEventType.Verbose);

	        Common.fpDebugMessage("strWhereUpd:", TraceEventType.Verbose);
	        Common.fpDebugMessage(strWhereUpd, TraceEventType.Verbose);

	        Common.fpDebugMessage("strListUpd:", TraceEventType.Verbose);
	        Common.fpDebugMessage(strListUpd, TraceEventType.Verbose);

	        Common.fpDebugMessage("identify FK(s)", TraceEventType.Information);

	        //Kimenő FK-k, amiket a beszúráshoz ki kell kapcsolni
	        tblLoader.SelectCommand.CommandText = "select constraint_schema, constraint_name from information_schema.table_constraints where constraint_type = 'FOREIGN KEY' and table_name = '" + strTableName + "'";

	        tblFK = new DataTable();
	        lngRowsCnt = tblLoader.Fill(tblFK);

	        Common.fpDebugMessage("return: " + lngRowsCnt.ToString(), TraceEventType.Verbose);
	        lngCols = tblFK.Rows.Count;

	        Common.fpDebugMessage("rows: " + lngCols.ToString(), TraceEventType.Verbose);

	        for (lngCnt1 = 0; lngCnt1 <= lngCols - 1; lngCnt1++) {
		        Common.fpDebugMessage(tblFK.Rows[(int)lngCnt1]["constraint_schema"] + "." + tblFK.Rows[(int)lngCnt1]["constraint_name"], TraceEventType.Information);

		        sb = new StringBuilder();
		        //sb.AppendFormat("alter table [{0}].[{1}] nocheck constraint [{2}].[{3}]", strSchemaName, strTableName, tblFK.Rows(lngCnt1).Item("constraint_schema"), tblFK.Rows(lngCnt1).Item("constraint_name"))
		        sb.AppendFormat("alter table [{0}].[{1}] nocheck constraint [{2}]", strSchemaName, strTableName, tblFK.Rows[(int)lngCnt1]["constraint_name"]);
		        strFkOut = strFkOut + sb.ToString() + Environment.NewLine;

		        sb = new StringBuilder();
		        //sb.AppendFormat("alter table [{0}].[{1}] check constraint [{2}].[{3}]", strSchemaName, strTableName, tblFK.Rows(lngCnt1).Item("constraint_schema"), tblFK.Rows(lngCnt1).Item("constraint_name"))
		        sb.AppendFormat("alter table [{0}].[{1}] check constraint [{2}]", strSchemaName, strTableName, tblFK.Rows[(int)lngCnt1]["constraint_name"]);
		        strFkIn = strFkIn + sb.ToString() + Environment.NewLine;

	        }

	        Common.fpDebugMessage(strFkOut, TraceEventType.Verbose);
	        Common.fpDebugMessage(strFkIn, TraceEventType.Verbose);

	        //Bejövő FK-k, amiket törléshez ki kell kapcsolni
	        sb = new StringBuilder();
	        sb.Append("select constraint_schema,constraint_name,object_name(parent_object_id) constraint_table_name,p.schema_name,p.table_name from ");
	        sb.Append("INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc ");
	        sb.Append("inner join sys.objects o on o.name collate database_default = rc.CONSTRAINT_NAME collate database_default ");
	        sb.Append("inner join sys.schemas s on s.schema_id=o.schema_id ");
	        sb.Append("inner join schVer.vewSds sc on sc.schema_id = o.schema_id ");
	        sb.Append("inner join schVer.vewSds su on su.name = rc.UNIQUE_CONSTRAINT_SCHEMA ");
	        sb.Append("inner join (");
	        sb.Append("select ");
	        sb.Append("o.name pk_name ");
	        sb.Append(",s.name schema_name ");
	        sb.Append(",object_name(parent_object_id) table_name ");
	        sb.Append("from ");
	        sb.Append("sys.objects o ");
	        sb.Append("inner join sys.schemas s on s.schema_id=o.schema_id ");
	        sb.Append("where ");
	        sb.Append("o.type_desc LIKE '%CONSTRAINT' ");
	        sb.Append("and ");
	        sb.Append("o.type in ('PK') ");
	        sb.Append(") p on p.pk_name  collate database_default = rc.UNIQUE_CONSTRAINT_NAME collate database_default ");
	        sb.Append("and ");
	        sb.Append("p.schema_name collate database_default = rc.UNIQUE_CONSTRAINT_SCHEMA collate database_default ");
	        sb.Append("where ");
	        sb.AppendFormat("p.schema_name = '{0}' ", strSchemaName);
	        sb.Append("and ");
	        sb.AppendFormat("p.table_name = '{0}' ", strTableName);
	        Common.fpDebugMessage("FkIn qry: " + sb.ToString(), TraceEventType.Verbose);

	        tblLoader.SelectCommand.CommandText = sb.ToString();
	        DataTable tblFkIn = new DataTable();
	        int lngRowsFkInCnt = tblLoader.Fill(tblFkIn);

	        Common.fpDebugMessage("return: " + lngRowsFkInCnt.ToString(), TraceEventType.Verbose);
	        int lngColsFkIn = tblFkIn.Rows.Count;

	        Common.fpDebugMessage("rows: " + lngColsFkIn.ToString(), TraceEventType.Verbose);

	        string strFkIn_Out = string.Empty;
	        string strFkIn_In = string.Empty;

	        for (lngCnt1 = 0; lngCnt1 <= lngColsFkIn - 1; lngCnt1++) {
		        Common.fpDebugMessage(tblFkIn.Rows[(int)lngCnt1]["constraint_schema"] + "." + tblFkIn.Rows[(int)lngCnt1]["constraint_table_name"] + "." + tblFkIn.Rows[(int)lngCnt1]["constraint_name"], TraceEventType.Information);

		        sb = new StringBuilder();
		        sb.AppendFormat("alter table [{0}].[{1}] nocheck constraint [{2}]", tblFkIn.Rows[(int)lngCnt1]["constraint_schema"], tblFkIn.Rows[(int)lngCnt1]["constraint_table_name"], tblFkIn.Rows[(int)lngCnt1]["constraint_name"]);
		        strFkIn_Out = strFkIn_Out + sb.ToString() + Environment.NewLine;

		        sb = new StringBuilder();
		        sb.AppendFormat("alter table [{0}].[{1}] check constraint [{2}]", tblFkIn.Rows[(int)lngCnt1]["constraint_schema"], tblFkIn.Rows[(int)lngCnt1]["constraint_table_name"], tblFkIn.Rows[(int)lngCnt1]["constraint_name"]);
		        strFkIn_In = strFkIn_In + sb.ToString() + Environment.NewLine;

	        }

	        Common.fpDebugMessage(strFkIn_Out, TraceEventType.Verbose);
	        Common.fpDebugMessage(strFkIn_In, TraceEventType.Verbose);

	        Common.fpDebugMessage("load data table", TraceEventType.Information);

	        strQueryUpd = strListUpd + " from deleted t1 inner join inserted t2 on " + strJoin;
	        //& strWhereUpd
	        strQueryIns = strListIns + " from inserted t1";
	        strQueryDel = strListDel + " from deleted t2";

	        strQuery = Environment.NewLine;

	        switch (triggContext.TriggerAction) {
		        case TriggerAction.Delete:
			        Common.fpDebugMessage("TriggerAction: Delete", TraceEventType.Information);
			        Common.fpDebugMessage("Query: " + strQueryDel, TraceEventType.Information);
			        tblLoader.SelectCommand.CommandText = strQueryDel;
			        tblModifyData = new DataTable();

			        lngRowsCnt = tblLoader.Fill(tblModifyData);

			        Common.fpDebugMessage("return: " + lngRowsCnt.ToString(), TraceEventType.Verbose);
			        lngCols = tblModifyData.Rows.Count;
			        lngRowCount = lngCols;

			        Common.fpDebugMessage("rows: " + lngCols.ToString(), TraceEventType.Verbose);

			        if (lngCols > 0) {
				        strQuery = strQuery + strFkIn_Out + Environment.NewLine;

				        for (lngCnt1 = 0; lngCnt1 <= lngCols - 1; lngCnt1++) {
					        strDelRunWhere = string.Empty;
					        rowCurrent = tblModifyData.Rows[(int)lngCnt1];
					        foreach (DataColumn fld in tblModifyData.Columns) {
						        sb = new StringBuilder();
						        sb.AppendFormat("({0}={1})", fld.ColumnName, mstrFieldToValue(rowCurrent[fld.Ordinal]));
						        strDelRunWhere = strDelRunWhere + (strDelRunWhere.Length > 0 ? " and " : "") + sb.ToString();
					        }
					        sb = new StringBuilder();
					        sb.AppendFormat("delete [{0}].[{1}] where {2}", strSchemaName, strTableName, strDelRunWhere);
					        strDelRun = sb.ToString();

					        strQuery = strQuery + strDelRun + Environment.NewLine;

				        }

				        strQuery = strQuery + strFkIn_In + Environment.NewLine;
			        }

			        break;
		        case TriggerAction.Update:
			        strQuery = strQuery + strFkOut;
			        Common.fpDebugMessage("TriggerAction: Update", TraceEventType.Information);
			        tblLoader.SelectCommand.CommandText = strQueryUpd;
			        tblModifyData = new DataTable();
			        lngRowsCnt = tblLoader.Fill(tblModifyData);
			        Common.fpDebugMessage("return: " + lngRowsCnt.ToString(), TraceEventType.Verbose);
			        lngCols = tblModifyData.Rows.Count;

			        Common.fpDebugMessage("rows: " + lngCols.ToString(), TraceEventType.Verbose);


			        if (lngCols > 0) {
				        DataColumn fld1 = default(DataColumn);
				        DataColumn fld2 = default(DataColumn);


				        for (lngCnt1 = 0; lngCnt1 <= lngCols - 1; lngCnt1++) {
					        Common.fpDebugMessage("row: " + lngCnt1.ToString(), TraceEventType.Verbose);

					        rowCurrent = tblModifyData.Rows[(int)lngCnt1];

					        strUpdRunList = string.Empty;
					        strUpdRunWhere = string.Empty;

					        lngZ = 0;

					        foreach (DataColumn fld in tblModifyData.Columns) {
						        //strFldName = Strings.Right(fld.ColumnName, fld.ColumnName.Length - 3);
                                strFldName = fld.ColumnName.Substring(3);
						        Common.fpDebugMessage("Field: " + strFldName, TraceEventType.Verbose);
						        if (Math.Ceiling((decimal)(lngZ / 2)) == lngZ / 2) {
							        //Páros
							        fld2 = fld;
						        } else {
							        //Páratlan
							        fld1 = fld;

							        //Dolgozunk
							        if (false == rowCurrent[fld1.Ordinal].Equals(rowCurrent[fld2.Ordinal])) {
								        sb = new StringBuilder();
								        sb.AppendFormat("[{0}]={1}", strFldName, mstrFieldToValue(rowCurrent[fld1.Ordinal]));
								        strUpdRunList = strUpdRunList + (strUpdRunList.Length > 0 ? ", " : "") + sb.ToString();
								        Common.fpDebugMessage("Not equal: ", TraceEventType.Verbose);
								        Common.fpDebugMessage("Field1: " + rowCurrent[fld1.Ordinal].ToString(), TraceEventType.Verbose);
								        Common.fpDebugMessage("Field2: " + rowCurrent[fld2.Ordinal].ToString(), TraceEventType.Verbose);
							        } else {
								        Common.fpDebugMessage("Equal: " + rowCurrent[fld1.Ordinal].ToString(), TraceEventType.Verbose);
							        }
							        if (strJoin.Contains(strFldName)) 
                                    {
								        sb = new StringBuilder();
								        sb.AppendFormat("([{0}]={1})", strFldName, mstrFieldToValue(rowCurrent[fld1.Ordinal]));
								        //Key-ek
								        strUpdRunWhere = strUpdRunWhere + (strUpdRunWhere.Length > 0 ? " and " : "") + sb.ToString();
							        }
						        }
						        lngZ = lngZ + 1;
					        }


					        if (strUpdRunList.Length > 0) {
						        Common.fpDebugMessage("strUpdRunList: " + strUpdRunList, TraceEventType.Verbose);
						        Common.fpDebugMessage("strUpdRunWhere: " + strUpdRunWhere, TraceEventType.Verbose);

						        lngRowCount += 1;

						        sb = new StringBuilder();
						        sb.AppendFormat("update [{0}].[{1}] set {2} where {3}", strSchemaName, strTableName, strUpdRunList, strUpdRunWhere);
						        strUpdRun = sb.ToString();
					        } else {
						        strUpdRun = "--nem változott semmilyen mező";
					        }

					        strQuery = strQuery + strUpdRun + Environment.NewLine;
					        lngCnt1 = lngCnt1 + 1;
				        }

				        strQuery = strQuery + strFkIn + Environment.NewLine;
			        }

			        break;
		        case TriggerAction.Insert:

			        Common.fpDebugMessage("TriggerAction: Insert", TraceEventType.Information);
			        Common.fpDebugMessage("Query: " + strQueryIns, TraceEventType.Information);
			        tblLoader.SelectCommand.CommandText = strQueryIns;
			        tblModifyData = new DataTable();
			        lngRowsCnt = tblLoader.Fill(tblModifyData);

			        Common.fpDebugMessage("return: " + lngRowsCnt.ToString(), TraceEventType.Verbose);
			        lngCols = tblModifyData.Rows.Count;
			        lngRowCount = lngCols;

			        Common.fpDebugMessage("rows: " + lngCols.ToString(), TraceEventType.Verbose);


			        if (lngCols > 0) {
				        strQuery = strQuery + strFkOut;
				        strQuery = strQuery + strIdOn + Environment.NewLine;


				        for (lngCnt1 = 0; lngCnt1 <= lngCols - 1; lngCnt1++) {
					        strInsRunFields = string.Empty;
					        strInsRunValues = string.Empty;
					        rowCurrent = tblModifyData.Rows[lngCnt1];
					        foreach (DataColumn fld in tblModifyData.Columns) {
						        strInsRunFields = strInsRunFields + (strInsRunFields.Length > 0 ? ", " : "") + fld.ColumnName;
						        strInsRunValues = strInsRunValues + (strInsRunValues.Length > 0 ? ", " : "") + mstrFieldToValue(rowCurrent[fld.Ordinal]);
					        }
					        sb = new StringBuilder();
					        sb.AppendFormat("insert [{0}].[{1}] ({2}) values ({3})", strSchemaName, strTableName, strInsRunFields, strInsRunValues);
					        strInsRun = sb.ToString();

					        strQuery = strQuery + strInsRun + Environment.NewLine;

				        }

				        strQuery = strQuery + strIdOff + Environment.NewLine;
				        strQuery = strQuery + strFkIn + Environment.NewLine;
			        }

			        break;
		        default:
                    Common.fpDebugMessage("TriggerAction: else (" + triggContext.TriggerAction.ToString() + ")", TraceEventType.Error);
			        throw new System.Exception("TriggerAction: else (" + triggContext.TriggerAction.ToString() + ") on table:" + strTableName);
	        }


	        Common.fpDebugMessage("query:", TraceEventType.Verbose);

	        Common.fpDebugMessage(strQuery, TraceEventType.Verbose);


	        if (lngRowCount > 0) {
		        Common.fpDebugMessage("insert to ztblVer", TraceEventType.Verbose);

		        command = new SqlCommand();
		        command.CommandText = "schVer.sprVerIns";
		        command.CommandType = CommandType.StoredProcedure;
		        command.Connection = dbConnection;

		        command.Parameters.Add("@strVerText", SqlDbType.NVarChar, -1);
		        //command.Parameters.Add("@strVerType", SqlDbType.NVarChar, 200)
		        //command.Parameters.Add("@strVerLog", SqlDbType.NVarChar, 200)
		        //command.Parameters.Add("@dtmVerDat", SqlDbType.DateTime)
		        //command.Parameters.Add("@strVerUser", SqlDbType.NVarChar, 100)

		        command.Prepare();

		        command.Parameters["@strVerText"].Value = strQuery;
		        //command.Parameters("@strVerType").Value = "TRIGGER"
		        //command.Parameters("@strVerLog").Value = "LOG"
		        //command.Parameters("@dtmVerDat").Value = Now()
		        //command.Parameters("@strVerUser").Value = "VB.NET"

		        command.ExecuteNonQuery();
	        } else {
                Common.fpDebugMessage("nothing to log", TraceEventType.Stop);
	        }

        #if DEBUG
            Common.fpDebugMessage("exiting from trgTable_Modify", TraceEventType.Stop);
        #endif

        }

        public static string mstrFieldToValue(object value)
        {

            string strTmp = null;

            switch (value.GetType().Name)
            {
                case "System.String":
                    strTmp = "'" + value.ToString() + "'";
                    break;
                case "System.Int32":
                    strTmp = value.ToString();
                    break;
                case "System.Int64":
                    strTmp = value.ToString();
                    break;
                case "System.DateTime":
                    strTmp = "'" + value.ToString() + "'";
                    break;
                case "System.DateTimeOffset":
                    strTmp = "'" + value.ToString() + "'";
                    break;
                case "System.Decimal":
                    strTmp = value.ToString().Replace(",", ".");
                    break;
                case "System.Single":
                    strTmp = value.ToString().Replace(",", ".");
                    break;
                case "System.Boolean":
                    if (true == Convert.ToBoolean(value))
                    {
                        strTmp = "1";
                    }
                    else
                    {
                        strTmp = "0";
                    }
                    break;
                case "System.DBNull":
                    strTmp = "Null";
                    break;
                case "System.Guid":
                    strTmp = "'" + value.ToString() + "'";
                    break;
                default:
                    Common.fpDebugMessage("type is not known: " + value.GetType().Name, TraceEventType.Error);
                    throw new System.Exception("type is not known: " + value.GetType().Name);
            }

            return strTmp;

        }
    }
}
