//Imports System
//Imports System.Data
using System.Data.Sql;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Data.SqlClient;
//#Const log = "NO"

namespace dmlt
{

    static class Common
    {
#if DEBUG
        static internal void fpDebugMessage(string vstrMessage, System.Diagnostics.TraceEventType vseverity)
        {
            SqlContext.Pipe.Send(string.Format("({0}):{1}",vseverity,vstrMessage));
            //My.Application.Log.WriteEntry("sdbTblTrg: " & vstrMessage, vseverity)
        }
#endif
    }
}
