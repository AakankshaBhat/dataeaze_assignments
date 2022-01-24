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
