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

public class StockWinner {
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
			Dataset<Row> hourlydata = session.createDataFrame(distFile, String.class);
			Dataset<Row> biggestwinners = calculateBiggestWinner(hourlydata, session);
			
			//Required first dataframe to calculate biggestwinner for each hour.
			//The following for loop continually appends hourlywinner to biggestwinner dataframe. 
			
			for (int j = 1; j < 24; j++) {
			
				s3filename = s3bucketname + next + "/" + next + 
					"_BINS_XETRA" + String.format("%02d", j) + ".csv";									
				distFile = context.textFile(s3filename);
				
				//Create dataframes for each s3 file, runs biggest winner query on each file.
				
				hourlydata = session.createDataFrame(distFile, String.class);
				Dataset<Row> hourlywinner = calculateBiggestWinner(hourlydata, session);
				biggestwinners = hourlywinner.union(biggestwinners);
				
			}
			
			//Display biggest stock winners of the day, in order. Max of 24 records shown. 
			
			aggregateHourlyWinners(biggestwinners, session).show();
		}
	}
	
	//Calculates biggest winner in terms of earnings. Assumed to be TradedVolume*(EndPrice-StartPrice).
	
	public static Dataset<Row> calculateBiggestWinner(Dataset<Row> data, SparkSession session) {
		data.createOrReplaceTempView("xetra");
		Dataset<Row> sqlDF = session.sql("SELECT Date, SecurityID, SecurityDesc, "
				+ "Max(Cast(TradedVolume as INT) * (Cast (EndPrice as INT) - Cast (StartPrice as INT)) as Earnings"
				+ "  FROM xetra");
		return sqlDF;
	}
	
	//Aggregates all hourly winners and finds the biggest stock winner for that day.
	
	public static Dataset<Row> aggregateHourlyWinners(Dataset<Row> biggestwinners, SparkSession session) {
		biggestwinners.createOrReplaceTempView("Aggregate");
		Dataset<Row> sqlDF = session.sql("SELECT Date, SecurityID, SecurityDesc, Sum(Cast(Earnings as INT)) from Aggregate"
				+ "group by SecurityID order by Cast(Earnings as INT) ASC");
		
		sqlDF.createOrReplaceTempView("Biggest_Winner");
		Dataset<Row> winner = session.sql("SELECT Date, SecurityID, SecurityDesc, Max(Cast(Earnings as INT)) from Biggest_Winner");
		return winner;
	}
}
