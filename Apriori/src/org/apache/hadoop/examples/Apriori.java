package org.apache.hadoop.examples;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
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
import org.mockito.internal.matchers.EndsWith;

import jdk.internal.org.objectweb.asm.tree.IntInsnNode;
import sun.awt.InputMethodSupport;

public class Apriori {
	//用于生成k+1候选集
	public static class GenerateMapper extends Mapper<Object, Text, Text, Text> {
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			if(value.toString().equals(""))
				return;
			
			String[] items = value.toString().split("\\s+")[0].split(",");
			String map_key = "";
			if(items.length > 1) {
				for(int i = 0; i < items.length - 1; i++) {
					if(i == items.length - 2)
						map_key += items[i];
					else
						map_key += items[i] + ","; //key: item1,item2,item3
				}
				context.write(new Text(map_key), new Text(items[items.length-1]));
			}
			else if(items.length == 1) {
				context.write(new Text(""), new Text(items[0]));
			}
			
		}
	}
	
	public static class GenerateReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			List<Integer> items = new ArrayList<>();
			for(Text val : values) {
				items.add(Integer.parseInt(val.toString()));
			}
			Collections.sort(items);
 			
 			for(int i = 0; i < items.size(); i++) {
 				for(int j = i + 1; j < items.size(); j++) {
 					String reduce_value = "";
 					if(key.toString().equals(""))
 						reduce_value = String.valueOf(items.get(i)) + "," +  String.valueOf(items.get(j));
 					else 
 						reduce_value = key.toString() + "," + String.valueOf(items.get(i)) + "," +  String.valueOf(items.get(j));
 					context.write(key, new Text(reduce_value));
 				}
 			}
		}
	}
	
	//k = 1
	public static class AprioriK1Mapper extends Mapper<Object, Text, Text, IntWritable> {
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			if(value.toString().equals(""))
				return;
			String[] fields = value.toString().split("\\s+");
			for(int i = 2; i <= 19; i++) {
				if(fields[i].equals("T"))
					context.write(new Text(String.valueOf(i - 1)), new IntWritable(1));
			}
				
		}
	}
	
	public static class AprioriK1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val : values)
				sum += val.get();
			if(sum >= 185)
				context.write(key, new IntWritable(sum));
		}
	}
	
	//k >= 2
	public static class AprioriMapper extends Mapper<Object, Text, Text, IntWritable> {
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			if(value.toString().equals(""))
				return;
			
			String[] fields = value.toString().split("\\s+");
//			for(int i = 0; i < fields.length; i++)
//				System.out.print(fields[i] + " ");

			String items_content = context.getConfiguration().get("items");
			System.out.println("items_content: " + items_content);
			String[] items_tuple = items_content.split("\\s+");
			for(int i = 0; i < items_tuple.length; i++) {
				String[] item = items_tuple[i].split(",");
				int flag = 1;
				for(int j = 0; j < item.length; j++) {
					int item_idx = Integer.parseInt(item[j]);
					if(fields[item_idx + 1].equals("F")) {
						flag = 0;
						break;
					}
				}
				if(flag == 1 && !items_tuple[i].equals("")) {
					context.write(new Text(items_tuple[i]), new IntWritable(1));
				}
			}	
		}
	}
	
	public static class AprioriReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val : values)
				sum += val.get();
			if(sum >= 185)
				context.write(key, new IntWritable(sum));
		}
	}
	
	public static void getItems(String path_str, Configuration configuration) throws IOException {
		FileSystem fileSystem = FileSystem.get(configuration);
		Path file = new Path(path_str);
		FSDataInputStream inputStream = fileSystem.open(file);
		String line = "";
		String items = "";
		while((line = inputStream.readLine()) != null) {
			items += line.split("\\s+")[1] + " ";
		}
		System.out.println(items);
		configuration.set("items", items);
		inputStream.close();
	}
	
	public static void main(String[] args)
			throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		Configuration conf = new Configuration();
		
		for (int i = 0; i < 3; i++) {
			if (i == 0) {
				String pathIn1 = "input/Apriori"; // 输入路径
				String pathOut1 = "output/Apriori"; // 输出路径

				Job job = Job.getInstance(conf, "Apriori");
				job.setJarByClass(Apriori.class);
				job.setMapperClass(AprioriK1Mapper.class);
				job.setReducerClass(AprioriK1Reducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(IntWritable.class);

				FileSystem.get(job.getConfiguration()).delete(new Path(pathOut1), true);
				FileInputFormat.addInputPath(job, new Path(pathIn1));
				FileOutputFormat.setOutputPath(job, new Path(pathOut1));
				job.waitForCompletion(true);
			} else {
				String pathIn1 = "input/Apriori"; // 输入路径
				String pathOut1 = "output/Apriori"; // 输出路径,输入k项频繁项集

				Job job = Job.getInstance(conf, "Apriori");
				job.setJarByClass(Apriori.class);
				job.setMapperClass(AprioriMapper.class);
				job.setReducerClass(AprioriReducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(IntWritable.class);

				FileSystem.get(job.getConfiguration()).delete(new Path(pathOut1), true);
				FileInputFormat.addInputPath(job, new Path(pathIn1));
				FileOutputFormat.setOutputPath(job, new Path(pathOut1));
				job.waitForCompletion(true);
			}

			{
				String pathIn1 = "output/Apriori"; // 输入路径,输入为k次的频繁项集
				String pathOut1 = "output/Apriori1"; // 输出路径，输出为ｋ+1次候选集

				Job job = Job.getInstance(conf, "Apriori");
				job.setJarByClass(Apriori.class);
				job.setMapperClass(GenerateMapper.class);
				job.setReducerClass(GenerateReducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);

				FileSystem.get(job.getConfiguration()).delete(new Path(pathOut1), true);
				FileInputFormat.addInputPath(job, new Path(pathIn1));
				FileOutputFormat.setOutputPath(job, new Path(pathOut1));
				job.waitForCompletion(true);
				
				getItems("output/Apriori1/part-r-00000", conf);
			}
			
		}

	}
}
