package com.gnguyen.Xetra_analysis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import com.gnguyen.Util.*;


public class BiggestVolumePerDayDriver {
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Biggest_Volume_per_Day");
		JavaSparkContext context = new JavaSparkContext(conf);
		SparkSession session = new SparkSession(context.sc());

		BiggestVolumePerDay.createDataframes(context, session);

		session.close();	
		context.close();	
	}
}
