package com.gnguyen.Util;

import java.time.LocalDate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;

public class BiggestVolumePerDay {
public static void createDataframes(JavaSparkContext context, SparkSession session) {
		
		//Creates LocalDate range January 1st of 2018 to December 31st of 2018.
		
		String s3bucketname = "s3a://deutsche-boerse-xetra-pds/";	
		LocalDate start = LocalDate.parse("2018-01-01");
		LocalDate end = LocalDate.parse("2018-12-31");
		LocalDate next = start.minusDays(1);
		
		//Uses file names to access s3 csv files, then creates RDDs of each unique file. 
		
		while ((next = next.plusDays(1)).isBefore(end.plusDays(1))) {
				
			//Creates first dataframe from s3 files for that day. This represents dataframe of first hour for each day. 
			
			String s3filename = s3bucketname + next + "/" + next + 
				"_BINS_XETRA00.csv";
			
			JavaRDD<String> distFile = context.textFile(s3filename);
			Dataset<Row> aggregateData = session.createDataFrame(distFile, String.class);
			
			//Required first dataframe so that subsequent ones created in the loop can be appended to it.
			//Aggregate all datasets per day so that resulting dataset contains highest volume and lowest volume per stock.
			for (int j = 1; j < 24; j++) {
				
				s3filename = s3bucketname + next + "/" + next + 
						"_BINS_XETRA" + String.format("%02d", j) + ".csv";									
				distFile = context.textFile(s3filename);
				
				Dataset<Row> hourlydata = session.createDataFrame(distFile, String.class);
				
				aggregateData = hourlydata.union(aggregateData);
				aggregateData.createOrReplaceTempView("xetra");
				
			}
			
			aggregateData.createOrReplaceTempView("high_volume");
			Dataset<Row> high = session.sql("Select SecurityID, Security Desc, Max(TradedVolume) as HighestVolume from high_volume"
					+ "group by SecurityID order by high_volume desc");
			aggregateData.createOrReplaceTempView("low_volume");
			Dataset<Row> low = session.sql("Select SecurityID, Security Desc, Min(TradedVolume) as LowestVolume from low_volume"
					+ "group by SecurityID order by low_volume asc");
			
			//Creates dataset with high and low volume value columns. 
			
			Dataset<Row> highandlow = high.join(low, "SecurityID");
			
			highandlow.createOrReplaceTempView("highest_implied_volume");
			
			//Find implied_volumes by averaging (high-low)/low for each stock
			
			Dataset<Row> highestImplied = session.sql("Select SecurityID, Security Desc, Avg((HigestVolume - LowestVolume)/LowestVolume) as implied_volume from highest_implied_volume"
					+ "group by SecurityID order by implied_volume asc");
			
			//Find largest implied_volume for all stocks that day
			
			highestImplied.createOrReplaceTempView("highest_stock");
			session.sql("Select SecurityID, Security Desc, Max(implied_volume) from highest_stock").show();
		}
		
		
	}
}
