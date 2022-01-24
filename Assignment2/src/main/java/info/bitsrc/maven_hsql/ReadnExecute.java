import java.io.FileReader;
import com.opencsv.CSVReader;
import java.io.FileWriter;
import com.opencsv.CSVWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Scanner;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;

//Read CSV file
public class ReadFromCSV {
   public static void main(String args[]) throws Exception {
      //Instantiating the CSVReader class
      CSVReader reader = new CSVReader(new FileReader("C:/Users/91738/Desktop/Assignment/nonConfidential.csv"));
      //Reading the contents of the csv file
      StringBuffer buffer = new StringBuffer();
      String line[];
      while ((line = reader.readNext()) != null) {
         for(int i = 0; i<line.length; i++) {
            System.out.print(line[i]+" ");
         }
         System.out.println(" ");
      }
   }
}

//Read Parquet File
public class ReadFromParquet {

    public static void main(String[] args) throws IOException {

      Path file = new Path("C:/Users/91738/Desktop/Assignment/confidential.snappy.parquet");


      List<Long> readTimes = new ArrayList<Long>();
      List<GenericRecord> allRecords = new ArrayList<GenericRecord>();
      Schema schema = null;

      for(int i = 0; i < 11; i++) {

         //read
         TimeWatch readTime = TimeWatch.start();
         ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(file).build();
         GenericRecord record;
         while((record = reader.read()) != null) {
            if(i == 0) {
               //add once
               allRecords.add(record);
               if(schema == null) {
                  schema = record.getSchema();
               }
            }
         }
         reader.close();
         long readMs = readTime.time();
         if(i != 0) {
            readTimes.add(readMs);
         }
   }
}

   
//Make connection to HSQL DB and execute queries
public void testDatabase() throws Exception {
   Connection conn = null;
        String db = "jdbc:hsqldb:hsql://localhost/testdb;ifexists=true";
        String user = "SA";
        String password = "";
         
            
            // Create and execute statement
            //Statement stmt = conn.createStatement();
            //ResultSet rs =  stmt.executeQuery("select FIRSTNAME, LASTNAME from CUSTOMER");
       
    Database database = new Database();
    Path path1 = Paths.get("C: /Users/91738/Desktop/Assignment/nonConfidential.csv ");
    database.read(path1);
    Path path2 = Paths.get("C:/Users/91738/Desktop/Assignment/confidential.snappy.parquet");
    database.read(path2);

   //Create DB connection
    Connection connection = DriverManager.getConnection(db, user, password);
    Statement statement = connection.createStatement();

    String selectQuery = "select * from " + database.getTableName(path1);
    ResultSet resultSet = statement.executeQuery(selectQuery);
    printResultSet(database, path1, resultSet);

    String selectQuery2 = "select * from " + database.getTableName(path2);
    ResultSet resultSet2 = statement.executeQuery(selectQuery2);
    printResultSet(database, path2, resultSet2);

    String selectQuery3 = "select count(*) from " + database.getTableName(path1)
            + " join " + database.getTableName(path2)
            + " on " + database.getTableName(path1)
            + "." + database.getColumnNames(path1).get(8)
            + "." + database.getColumnNames(path2).get(8) + "where" + database.getColumnNames(path2).get(5) +"."+ database.getColumnNames(path2).get(5)+"IN ("VA")";
    ResultSet resultSet3 = statement.executeQuery(selectQuery3);
   
    String csvFilePath = "1.csv";
    BufferedWriter fileWriter = new BufferedWriter(new FileWriter(csvFilePath));
    while (resultSet3.next()) {
        StringBuilder result = new StringBuilder();
         
        result.append(resultSet3.getString(
                database.getColumnNames(path1).get(5))).append(" ");
        result.append(resultSet3.getString(
                database.getColumnNames(path2).get(8))).append(" ");
    
        //System.out.println(result.toString());
        File.WriteAllText(csvFilePath, result.toString());
    }

   
     String selectQuery3 = "select count(*) from " + database.getTableName(path1)
            + " join " + database.getTableName(path2)
            + " on " + database.getTableName(path1)
            + "." + database.getColumnNames(path1).get(8)
            + "." + database.getColumnNames(path2).get(8) + "where" + database.getColumnNames(path1).get(5) +"."+ database.getColumnNames(path2).get(5)+"IN ("VA")";
    ResultSet resultSet3 = statement.executeQuery(selectQuery3);
   
   //Number of LEED projects there in Virginia
    String csvFilePath = "1.csv";
    BufferedWriter fileWriter = new BufferedWriter(new FileWriter(csvFilePath));
    while (resultSet3.next()) {
        StringBuilder result = new StringBuilder();
         
        result.append(resultSet3.getString(
                database.getColumnNames(path1).get(5))).append(" ");
        result.append(resultSet3.getString(
                database.getColumnNames(path2).get(8))).append(" ");
    
        //System.out.println(result.toString());
        File.WriteAllText(csvFilePath, result.toString());
    }
   
   //#Number of LEED projects in Virginia by owner type
     String selectQuery4 = "select count(*) from " + database.getTableName(path1)
            + " join " + database.getTableName(path2)
            + " on " + database.getTableName(path1)
            + "." + database.getColumnNames(path1).get(13)
            + "." + database.getColumnNames(path2).get(13) + "where" + database.getColumnNames(path1).get(5) +"."+ database.getColumnNames(path2).get(5)+"IN ('VA') groupby" +database.getColumnNames(path1).get(13);
    ResultSet resultSet4 = statement.executeQuery(selectQuery4);
   
    String csvFilePath2 = "2.csv";
    BufferedWriter fileWriter = new BufferedWriter(new FileWriter(csvFilePath));
    while (resultSet4.next()) {
        StringBuilder result = new StringBuilder();
         
        result.append(resultSet4.getString(
                database.getColumnNames(path1).get(5))).append(" ");
        result.append(resultSet4.getString(
                database.getColumnNames(path1).get(13))).append(" ");
    
        //System.out.println(result.toString());
        File.WriteAllText(csvFilePath2, result.toString());
    }
   
   //Total Gross Square Feet of building space that is LEED-certified in Virginia
     String selectQuery5 = "select sum(GrossSqFoot) from " + database.getTableName(path1)
            + " join " + database.getTableName(path2)
            + " on " + database.getTableName(path1)
            + "." + database.getColumnNames(path1).get(12)
            + "." + database.getColumnNames(path2).get(12) + "where" + database.getColumnNames(path1).get(5) +"."+ database.getColumnNames(path2).get(5)+"IN ('VA')"
            +"." + database.getColumnNames(path2).get(12)+"IN('YES')";
    ResultSet resultSet5 = statement.executeQuery(selectQuery3);
  
    String csvFilePath3 = "3.csv";
    BufferedWriter fileWriter = new BufferedWriter(new FileWriter(csvFilePath));
    while (resultSet5.next()) {
        StringBuilder result = new StringBuilder();
         
        result.append(resultSet5.getString(
                database.getColumnNames(path1).get(5))).append(" ");
        result.append(resultSet5.getString(
                database.getColumnNames(path2).get(8))).append(" ");
    
        //System.out.println(result.toString());
        File.WriteAllText(csvFilePath3, result.toString());
    }
    statement.close();
    fileWriter.close();
    connection.close();
}
