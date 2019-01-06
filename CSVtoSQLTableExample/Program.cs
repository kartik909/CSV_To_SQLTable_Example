using Microsoft.VisualBasic.FileIO;
using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;

namespace CSVtoSQLTableExample
{
    class Program
    {
        static void Main(string[] args)
        {
            //** To get any one file
            string csv_file_path = @"C:\Users\UserName\Documents\Sample.csv";  // File Path

            //** To get all the files from directory

            //DirectoryInfo di = new DirectoryInfo(@"C:\Users\UserName\Documents\");
            //FileInfo[] files = di.GetFiles(" *.csv");


            DataTable csvData = GetDataTabletFromCSVFile(csv_file_path);  

            Console.WriteLine("Rows count:" + csvData.Rows.Count);
            Console.WriteLine("Column count: " +csvData.Columns.Count);

            InsertDataIntoSQLServerUsingSQLBulkCopy(csvData); 
            Console.ReadLine();
        }

        //** To Get the DataTable from CSV File
        private static DataTable GetDataTabletFromCSVFile(string csv_file_path)
        {
            DataTable csvData = new DataTable();
           
            try
            {
                using (TextFieldParser csvReader = new TextFieldParser(csv_file_path))
                {
                    csvReader.SetDelimiters(new string[] { "," });
                    csvReader.HasFieldsEnclosedInQuotes = true;
                    string[] colFields = csvReader.ReadFields();

                    //** Adding the Columns

                    //foreach (string column in colFields)
                    //{

                    //    DataColumn datecolumn = new DataColumn(column);
                    //    datecolumn.AllowDBNull = true;
                    //    csvData.Columns.Add(column);
                    //}

                    //** To Add the Columns Externally 
                    csvData.Columns.Add("ColumnName 1", typeof(String));
                    csvData.Columns.Add("ColumnName 2", typeof(String));
                    csvData.Columns.Add("ColumnName 3", typeof(String));
                    csvData.Columns.Add("ColumnName 4", typeof(String));
                    csvData.Columns.Add("ColumnName 5", typeof(String));
                    csvData.Columns.Add("ColumnName 6", typeof(String));
                    csvData.Columns.Add("ColumnName 7", typeof(String));
                    csvData.Columns.Add("ColumnName 8", typeof(String));
                    csvData.Columns.Add("ColumnName 9", typeof(String));

                    //** To Adding the Rows
                    while (!csvReader.EndOfData)
                    {
                        string[] fieldData = csvReader.ReadFields();
                        string[] result = fieldData.Skip(1).ToArray();
                        //** Making empty value as null
                        for (int i = 0; i < result.Length; i++)
                        {
                            
                            result[i].Remove(0);
                            if (result[i] == "")
                            {
                               result[i] = null;
                            }
                      }
                        csvData.Rows.Add(result);
                       
                    }
                    //** To delete any Rows
                    // csvData.Rows.RemoveAt(0);
                    // csvData.Rows.RemoveAt(csvData.Rows.Count - 1);
                }
            }
            catch (Exception ex)
            {
                // Catch the Exception
            }
            return csvData;
        }

//** Method to insert Data in SQl Table
public static void InsertDataIntoSQLServerUsingSQLBulkCopy(DataTable csvFileData)
{
   
    using (SqlConnection dbConnection = new SqlConnection("Data Source=Server; Initial Catalog=DatabaseName; Integrated Security=True;"))
    {
        dbConnection.Open();
        using (SqlBulkCopy s = new SqlBulkCopy(dbConnection))
        {
            s.DestinationTableName = "TableTest";
                    int i = 0;
                    foreach (var column in csvFileData.Columns)
                    {
                        i++;

                        Console.WriteLine("Column " + i + " is :" +column);

                        s.ColumnMappings.Add(column.ToString(), column.ToString());
                    }

                    s.WriteToServer(csvFileData);
        }
    }
}
    }
}
