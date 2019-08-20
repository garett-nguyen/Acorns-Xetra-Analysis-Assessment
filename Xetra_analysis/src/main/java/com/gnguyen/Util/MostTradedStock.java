package com.gnguyen.Util;
import java.time.LocalDate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.time.YearMonth;
import java.time.LocalDate;
import java.time.MonthDay;

public class MostTradedStock {
	public static void createDataframes(JavaSparkContext context, SparkSession session) {
		
		//Creates LocalDate range January 1st of 2018 to December 31st of 2018.
		
		String s3bucketname = "s3a://deutsche-boerse-xetra-pds/";	
		LocalDate start = LocalDate.parse("2018-01-01");
		LocalDate end = LocalDate.parse("2018-12-31");
		LocalDate next = start.minusDays(1);
		
		//Uses file names to access s3 csv files, then creates RDDs of each unique file. 
		
		//Creates first file from s3. Will be used to concatenate subsequent datasets to for aggregation. Must be done once. 
		String s3filename = "s3a://deutsche-boerse-xetra-pds/2018-01-01/2018-01-01_BINS_XETRA00.csv";									
		JavaRDD<String> distFile = context.textFile(s3filename);
		Dataset<Row> aggregateData = session.createDataFrame(distFile, String.class);
		
		
		while ((next = next.plusDays(1)).isBefore(end.plusDays(1))) {
				
			for (int j = 0; j < 24; j++) {
				s3filename = s3bucketname + next + "/" + next + 
						"_BINS_XETRA" + String.format("%02d", j) + ".csv";		
				
				//Since the first s3 file was already made, only perform aggregation to s3 files that don't have the same name as the first. 
				if (!s3filename.equals("s3a://deutsche-boerse-xetra-pds/2018-01-01/2018-01-01_BINS_XETRA00.csv")) {
					distFile = context.textFile(s3filename);
					Dataset<Row> hourlydata = session.createDataFrame(distFile, String.class);
					
					
					aggregateData = hourlydata.union(aggregateData);
					aggregateData.createOrReplaceTempView("xetra");
					aggregateData = session.sql("Select SecurityID, SecurityDesc, Sum(Cast(TradedVolume as Int)) from xetra"
							+ "group by SecurityID");
					
				}			
			}
		}
		
		//Shows maximum traded volume after aggregation of all datasets in s3 bucket. 
		
		aggregateData.createOrReplaceTempView("Most_Traded_Volume");
		session.sql("Select SecurityID, SecurityDesc, MAX(Cast(TradedVolume as Int)) from xetra").show();
		
	}

}