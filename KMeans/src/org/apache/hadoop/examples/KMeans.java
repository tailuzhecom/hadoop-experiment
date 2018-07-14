package org.apache.hadoop.examples;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

public class KMeans {
	
	public static List<ArrayList<Float>> getCenters(String inputpath) {
		List<ArrayList<Float>> result = new ArrayList<ArrayList<Float>>();
		
		Configuration conf = new Configuration();
		try {
			FileSystem hdfs = FileSystem.get(conf);
			Path in = new Path(inputpath);
			FSDataInputStream fsInputStream = hdfs.open(in);
			LineReader lineIn = new LineReader(fsInputStream, conf);
			Text line = new Text();
			while(lineIn.readLine(line) > 0) {
				String record = line.toString();
				String[] fields = record.split("\\s+");
				List<Float> tmplist = new ArrayList<>();
				for(int i = 0; i < fields.length; i++) {
					tmplist.add(Float.parseFloat(fields[i]));
				}
				result.add((ArrayList<Float>)tmplist);
			}
			lineIn.close();
			fsInputStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		for(ArrayList<Float> vals : result) {
			for(Float val : vals) {
				System.out.print(String.valueOf(val) + " ");
			}
			System.out.println("");
		}
		
		return result;
	}
	
	public static class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {
	
		//更新种类
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			if(value.toString().equals(""))
				return;
			
			String[] fields = value.toString().split("\\s+");
			String[][] center_str = new String[3][];
			for(int i = 0; i < 3; i++) {
				center_str[i] = context.getConfiguration().get(String.valueOf(i)).toString().split("\\s+");
			}
			//获取center
			float[][] centers = new float[3][4];
			for(int i = 0; i < 3; i++) {
				for(int j = 0; j < 4; j++) {
					centers[i][j] = Float.parseFloat(center_str[i][j]);
					//System.out.print(centers[i][j] + " ");
				}
				//System.out.println("");
			}
			
			
			
			int index = 0;
			float min_distance = 0;
			for(int i = 0; i < 3; i++) {
				float distance = 0;
				for(int j = 0; j < 4; j++) {
					distance += Math.pow(centers[i][j] - Float.parseFloat(fields[j+1]), 2);
				}
				if(i == 0)
					min_distance = distance;
				else if(min_distance > distance) {
					min_distance = distance;
					index = i;
				}
			}
			
			
			String map_value = value.toString();
			//value格式：sequence + field1 + field2 + field3 + field4
			//System.out.println(map_value);
			
			context.write(new IntWritable(index), new Text(map_value));
		}
	}
	
	public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
			float[] sums = {0f, 0f, 0f, 0f};
			int data_num = 0;
			for(Text val : values) {
				String[] fields = val.toString().split("\\s+");
				int output_key = Integer.parseInt(fields[0]); // sequence
				//field1 + field2 + field3 + field4 + index
				String output_str = fields[1] + " " + fields[2] + " " + fields[3] + " " + fields[4] + " " + String.valueOf(key.get());
				for(int i = 1; i <= 4; i++) {
					sums[i-1] += Float.parseFloat(fields[i]);
				}
				data_num++;
				context.write(new IntWritable(output_key), new Text(output_str));
			}
			
			float[] center_num = new float[4];
			String center_str = "";
			for(int i = 0; i < 4; i++) {
				center_num[i] = sums[i] / data_num;
				center_str += String.valueOf(center_num[i]) + " ";
			}
			context.getConfiguration().set(String.valueOf(key.get()), center_str);
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		Configuration conf = new Configuration();
		
		conf.set("0", "4.9  3.0  1.4  0.2");
		conf.set("1", "4.9  2.4  3.3  1.0");
		conf.set("2", "6.4  2.8  5.6  2.2");
		String pathIn1 = "input/KMeans/1"; // 输入路径
		String pathOut1 = "output/KMeans"; //输出路径，第二次迭代开始作为第一次mapreduce的输入路径
		
		for (int i = 0; i < 100; i++) {
			Job job = Job.getInstance(conf, "KMeans");
			job.setJarByClass(KMeans.class);
			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);

			FileSystem.get(job.getConfiguration()).delete(new Path(pathOut1), true);
			FileInputFormat.addInputPath(job, new Path(pathIn1));
			FileOutputFormat.setOutputPath(job, new Path(pathOut1));

			job.waitForCompletion(true);
		}
		
	}
}
