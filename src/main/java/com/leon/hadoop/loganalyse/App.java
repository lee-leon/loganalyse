package com.leon.hadoop.loganalyse;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.commons.cli2.OptionException;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.CachingRecommender;
import org.apache.mahout.cf.taste.impl.recommender.slopeone.SlopeOneRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import com.huaban.analysis.jieba.*;


public class App 
{
	static final String inputFile = "/root/workplace/Hadoop-Devel/loganalyse/ml-1m/ratings.dat";
	static final String outputFile = "/root/workplace/Hadoop-Devel/loganalyse/ml-1m/ratings.csv";
	public static void main( String[] args ) throws IOException, TasteException, OptionException {
			CreateCsvRatingsFile();
			File ratingsFile = new File(outputFile);
			DataModel model = new FileDataModel(ratingsFile);
			CachingRecommender cachingRecommender = new CachingRecommender(new SlopeOneRecommender(model));
			for (LongPrimitiveIterator it =  model.getUserIDs(); it.hasNext();) {
				long userId = it.nextLong();
				List<RecommendedItem> recommendations = cachingRecommender.recommend(userId, 10);
				if (recommendations.size() == 0){
					System.out.print("User ");
					System.out.print(userId);
					System.out.println(": no recommendations");
				}
				for (RecommendedItem recommendedItem : recommendations) {
					System.out.print("User ");
					System.out.print(userId);
					System.out.print(": ");
					System.out.println(recommendedItem);
				}
			}	
		}
	
	private static void CreateCsvRatingsFile() throws FileNotFoundException, IOException {
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
		
		String line = null;
		String line2write = null;
		String [] temp;
		int i = 0;
		while ((line = br.readLine()) != null && i < 10000) {
			i++;
			temp = line.split("::");
			line2write = temp[0] + "," + temp[1];
			bw.write(line2write);
			bw.newLine();
			bw.flush();	
		}
		br.close();
		bw.close();
	}
}
