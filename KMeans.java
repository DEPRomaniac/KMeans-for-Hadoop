package com.kmeans;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

class Datapoint implements WritableComparable<Datapoint> {
	double x;
	double y;
	
	public Datapoint() {
		super();
	}
	
	public Datapoint(double x, double y) {
		super();
		this.x = x;
		this.y = y;
	}
	
	public Datapoint(Datapoint point) {
		super();
		this.x = point.x;
		this.y = point.y;
	}
	
	@Override
	public String toString() {
		return (this.x + "," + this.y);
	}

	public Datapoint get() {
		return this;
	}
	
	 @Override
	 public void write(DataOutput out) throws IOException {
	  out.writeInt(2);
	  out.writeDouble(this.x);
	  out.writeDouble(this.y);
	 }
	 
	 @Override
	 public void readFields(DataInput in) throws IOException {
	  this.x = in.readDouble();
	  this.y = in.readDouble();
	 }
	
	 @Override
	 public int compareTo(Datapoint a) {
	 
	  if ((this.x == a.x) && (this.y == a.y)){
		  return 0;
	  }
	  return 1;
	 }
}

public class KMeans {
	
	public static String outfolder = "";
	public static String infolder = "";
	public static String datafile = "/data.txt";
	public static String centfile = "/centroid.txt";
	public static String outputfile = "/part-00000";
	public static String txtdelim = ",";
	public static String delim = "\t| ";
	
	public static List<Datapoint> kCentroids = new ArrayList<Datapoint>();

	public static List<Datapoint> buildDatapoints(Path filepath) throws Exception{  
		FileSystem filesystem = FileSystem.get(new Configuration());
		BufferedReader reader = new BufferedReader(new InputStreamReader(filesystem.open(filepath)));
		List<Datapoint> center = new ArrayList<Datapoint>();
		String line = reader.readLine();
		
		while (line != null) {
			String[] temp2 = line.split(delim);
			String[] temp = temp2[0].split(txtdelim);
			Datapoint current = new Datapoint(Double.parseDouble(temp[0]),Double.parseDouble(temp[1]));
			center.add(current);
			line = reader.readLine();
		}
		reader.close();
		return center;
	}

	
	public static boolean checkConv(List<Datapoint> currCents, List<Datapoint> nextCents, int iter_count, int k) throws Exception{
		if (iter_count == 10){
			return true;
		}
		else {
			int index = 0;
			double dist = 0.0;
			while (index != k){
				if ((currCents.isEmpty() == false) && (nextCents.isEmpty() == false)){
					dist = Euclideandistance(currCents.get(index),nextCents.get(index));
					if (dist != 0.0){
						return false;
						}
					}
				index++;
				}
			}
		return true;
		}

	public static double Euclideandistance(Datapoint a, Datapoint b) throws IOException{
		double dist = 0.0;
		dist = Math.sqrt(Math.pow((b.x-a.x), 2) + Math.pow((b.y-a.y), 2));
		return Math.abs(dist); 
	}
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void configure(JobConf job) {
			try {
				
				Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
				if (cacheFiles != null && cacheFiles.length > 0) {
					String line = "";
					BufferedReader in = null;
					kCentroids.clear();
					try {
						in = new BufferedReader(new FileReader(cacheFiles[0].toString()));
						while ((line = in.readLine()) != null) {

						String[] temp2 = line.split(delim);
						String[] temp = temp2[0].split(txtdelim);

						Datapoint temppoint = new Datapoint(Double.parseDouble(temp[0]),Double.parseDouble(temp[1]));
						kCentroids.add(temppoint);
						}
					} catch (FileNotFoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					} finally {
						if (in != null) {
							try {
								in.close();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
				}
			} catch (IOException e) {
				System.err.println("Exception reading DistributedCache: " + e);
			}
		}

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		   String line = value.toString();
		   String txtdelim = ",";
		   String[] temp = line.split(txtdelim);
		   Datapoint point = new Datapoint(Double.parseDouble(temp[0]),Double.parseDouble(temp[1]));
		   double newdist = Double.MAX_VALUE;
		   double mindist = Double.MAX_VALUE;
		   Datapoint nearestCent = kCentroids.get(0);
			for (Datapoint center: kCentroids){
				newdist = Euclideandistance(point, center);
				if (Math.abs(newdist) < Math.abs(mindist)){
					mindist = newdist;
					nearestCent = center;
				}
			}
		   output.collect(new Text(nearestCent.toString()), new Text(point.toString()));
		 }
		}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Datapoint, Text> {
		 @Override
		 public void reduce(Text key, Iterator<Text> values, OutputCollector<Datapoint, Text> output, Reporter reporter) throws IOException {
		   	Datapoint newCenter = new Datapoint(0.0,0.0);
		   	Datapoint sum = new Datapoint(0.0,0.0);
		   	int count = 0;
			String points = "";
			while (values.hasNext()) {
				String line = values.next().toString();  
			    String[] temp = line.split(txtdelim);
				Datapoint d = new Datapoint(Double.parseDouble(temp[0]),Double.parseDouble(temp[1])); 
			    points = points + " " + d.toString();
			    sum.x = sum.x + d.x;
			    sum.y = sum.y + d.y;
			    ++count;
			}
		   newCenter.x = sum.x / count;
		   newCenter.y = sum.y / count;
		   output.collect(new Datapoint(newCenter), new Text(points));
		 }
	}

	public static void main(String[] args) throws Exception {
		infolder = args[0];
		outfolder = args[1];
		int iter_count = 0;
		boolean isConverged = false;
		String previous = "";
		String output = outfolder + System.nanoTime();
		String input = output;
		while (isConverged == false) {
			JobConf conf = new JobConf(KMeans.class);
			if (iter_count == 0) {
				Path hdfsPath = new Path(infolder + centfile);
				DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
			} else {
				Path hdfsPath = new Path(input + outputfile);
				DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
			}

			conf.setJobName("KMeans");
			
			conf.setMapperClass(Map.class);
			conf.setMapOutputKeyClass(Text.class);
			conf.setMapOutputValueClass(Text.class);
			
			conf.setReducerClass(Reduce.class);
			
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputKeyClass(Datapoint.class);
			conf.setOutputValueClass(Text.class);
			conf.setOutputFormat(TextOutputFormat.class);

			FileInputFormat.setInputPaths(conf, new Path(infolder + datafile));
			FileOutputFormat.setOutputPath(conf, new Path(output));

			JobClient.runJob(conf);

			Path outfile_path = new Path(output + outputfile);
			
			List<Datapoint> nextCents = buildDatapoints(outfile_path);
			
			if (iter_count == 0) {
				previous = infolder + centfile;
			} else {
				previous = input + outputfile;
			}
			Path prevfile_path = new Path(previous);
			List<Datapoint> currCents = buildDatapoints(prevfile_path);
			int k = currCents.size();
			
			isConverged = checkConv(currCents, nextCents, iter_count, k);
			++iter_count;
			
			input = output;
			output = outfolder + System.nanoTime();
		}
	}
}