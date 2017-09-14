import java.io.IOException;

import java.util.*;



import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.*;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class Mapp {

	public static class Map extends

			Mapper<LongWritable, Text, Text, Text> {

		@Override

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();

			String splitline[] = line.split("::");

			// Get the movie & rating and store as string

			String moviename = splitline[1];

			String rating = splitline[6];

			String combined = moviename + "::" + rating;

			String genre = splitline[2];

			// Check for multiple genre's

			String splitgenre[] = genre.split(",");

			for (int i = 1; i < splitgenre.length; i++) {

				context.write(new Text(splitgenre[i]), new Text(combined));

			}

		}

	}

	// key-Action;value- ironman::4

	// key-Adventure;value- ironman::4

	public static class Reduce extends

			Reducer<Text, Text, Text, Text> {

		@Override

		public void reduce(Text key, Iterable<Text> values,

				Context context) throws IOException, InterruptedException {

			List<String> movielist = new ArrayList<String>();

			List<String> ratinglist = new ArrayList<String>();

			// get separated movies & ratings for a given genre

			for (Text val : values) {

				String strvalues = values.toString();

				String stringvalues[] = strvalues.split("::");

				movielist.add(stringvalues[0]);

				ratinglist.add(stringvalues[1]);

			}

			// get unique movies & count the corresponding rating

			List<String> uniquemovie = new ArrayList<String>();

			List<Double> ratingsum = new ArrayList<Double>();

			List<Integer> ratingcount = new ArrayList<Integer>();

			for (int i = 0; i <= movielist.size(); i++) {

				if (uniquemovie.contains(movielist.get(i))) {

					int index = uniquemovie.indexOf(movielist.get(i));

					ratingcount.set(index, ratingcount.get(index) + 1);

					ratingsum.set(index, ratingsum.get(index) + Double.parseDouble(ratinglist.get(i)));

				}

				else {

					uniquemovie.add(movielist.get(i));

					ratingcount.add(1);

					ratingsum.add(Double.parseDouble(ratinglist.get(i)));

					System.out.println();

				}

			}

			List<Double> ratingavg = new ArrayList<Double>();

			// compute average

			for (int j = 0; j <= uniquemovie.size(); j++) {

				double average = ratingsum.get(j) / ratingcount.get(j);

				ratingavg.add(average);

			}

			// get the highest average rating for a genre

			double tempavg = ratingavg.get(0);

			for (int k = 0; k <= ratingavg.size(); k++) {

				if ((ratingavg.get(k) > tempavg) && (ratingavg.get(k) != null)) {

					tempavg = ratingavg.get(k);

				}

			}

		}

	}

}
