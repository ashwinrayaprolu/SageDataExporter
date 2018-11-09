using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.IO;
using System.Collections;
#if NETCOREAPP2_1
    using System.Data.Odbc;
#else
    //using Microsoft.Data.Odbc;
#endif

//using Microsoft.Data.Entity;

/***

This code copies all data from SAGE to CSV Files for all tables in Given Company.
Usage:  dotnet run <DATASOURCE_NAME> <USER_NAME> <PASSWORD>


 */
namespace SageConnector
{
    class Program
    {


        private static OdbcConnection _cn;
        static void Main(string[] args)
        {

            #if NET40
                    Console.WriteLine("Target framework: .NET Framework 4.0");
            #elif NET45  
                    Console.WriteLine("Target framework: .NET Framework 4.5");
            #elif NETCOREAPP2_1
                    Console.WriteLine("Target framework: .NET CoreAPP 2.1");
            #else
                    Console.WriteLine("Target framework: .NET Standard 1.4");
            #endif


            

            if(args.Length != 3){

                Console.WriteLine( "Please enter arguments <DATASOURCE_NAME> <USER_NAME> <PASSWORD>" );

            }

            //"SOTAMAS90_silent"
            string dataSourceName = args[0];
            string userName = args[1];
            string password = args[2];
            
            //Console.WriteLine( "SizeOf IntPtr is: {0}", IntPtr.Size );
            
            Console.WriteLine("!!!Downloading Sage data!!!!");

            string connectionString = string.Format("DSN={0};UID={1};PWD={2};", dataSourceName,userName,password);

            List<String> tableNamesList = loadTableNames(connectionString);

            foreach(var tableName in tableNamesList )
            {
                string fileName = tableName;

                string query = "select * from "+fileName;

                Console.WriteLine("Processing for "+fileName);

                string filePath = "C:\\development\\Code\\Sage\\"+fileName+".csv";
                if(File.Exists(filePath)){
                    Console.WriteLine("Ignoring file : "+fileName);
                    continue;
                }

                
                DataSet dataSet = new DataSet();
                dataSet = GetDataSetFromAdapter(dataSet,connectionString,query);
                //CreateExcel(dataSet, "c:\\Demo.xls");

                string text = ConvertToCSV(dataSet);
                System.IO.File.WriteAllText(@"C:\\development\\Code\\Sage\\"+fileName+".csv", text);


            }


            
        }


        private static List<String> loadTableNames(string connectionString){
            var tableNames = new List<string>();

            using (OdbcConnection connection = 
					   new OdbcConnection(connectionString))
			{
                try
				{
					connection.Open();
					using(DataTable tableschema = connection.GetSchema("Tables"))
                    {
                        // first column name
                        foreach(DataRow row in tableschema.Rows)
                        {
                            tableNames.Add(row["TABLE_NAME"].ToString());
                            //Console.WriteLine(row["TABLE_NAME"].ToString());
                        }
                    }
				}
				catch (Exception ex)
				{
					Console.WriteLine(ex.Message);
				}

            }

            return tableNames;

        }


        public static DataSet GetDataSetFromAdapter(
			DataSet dataSet, string connectionString, string queryString)
		{
			using (OdbcConnection connection = 
					   new OdbcConnection(connectionString))
			{

                


				OdbcDataAdapter adapter = 
					new OdbcDataAdapter(queryString, connection);

				// Open the connection and fill the DataSet.
				try
				{
					connection.Open();
					adapter.Fill(dataSet);
				}
				catch (Exception ex)
				{
					Console.WriteLine(ex.Message);
				}
				// The connection is automatically closed when the
				// code exits the using block.
			}
			return dataSet;
		}        
        
        private static string ConvertToCSV(DataSet objDataSet)
        {
            StringBuilder content = new StringBuilder();

            if (objDataSet.Tables.Count >= 1)
            {
                DataTable table = objDataSet.Tables[0];

                if (table.Rows.Count > 0)
                {
                    DataRow dr1 = (DataRow) table.Rows[0];
                    int intColumnCount = dr1.Table.Columns.Count;
                    int index=1;

                    //add column names
                    foreach (DataColumn item in dr1.Table.Columns)
                    {
                        content.Append(String.Format("\"{0}\"", item.ColumnName));
                        if (index < intColumnCount)
                            content.Append(",");
                        else
                            content.Append("\r\n");
                        index++;
                    }

                    //add column data
                    foreach (DataRow currentRow in table.Rows)
                    {
                        string strRow = string.Empty;
                        for (int y = 0; y <= intColumnCount - 1; y++)
                        {
                            strRow += "\"" + currentRow[y].ToString() + "\"";

                            if (y < intColumnCount - 1 && y >= 0)
                                strRow += ",";
                        }
                        content.Append(strRow + "\r\n");
                    }
                }
            }

            return content.ToString();
        }


        private static void ProcessQuery(string query)
        {
			
            var cmd = new OdbcCommand(query, _cn);
            //OdbcDataAdapter adapter = new OdbcDataAdapter(cmd);
            DataSet dataSet = new DataSet();
            //adapter.Fill(dataSet);
            OdbcDataReader reader = cmd.ExecuteReader();
			DataTable schemaTable = reader.GetSchemaTable();

			foreach (DataRow row in schemaTable.Rows)
			{
				foreach (DataColumn column in schemaTable.Columns)
				{
					Console.WriteLine(String.Format("{0} = {1}",
					   column.ColumnName, row[column]));
				}
			}
			
			
			
			if (reader.HasRows)
			{
				while (reader.Read())
				{
					Console.WriteLine("{0}\t{1}", reader.GetString(0),
						reader.GetString(1));
				}
			}
			else
			{
				Console.WriteLine("No rows found.");
			}
			reader.Close();
	

        }
    }
}
