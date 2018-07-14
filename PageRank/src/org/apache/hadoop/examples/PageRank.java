package org.apache.hadoop.examples;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import jdk.internal.org.objectweb.asm.tree.IntInsnNode;


public class PageRank {
	public static enum counter// third
	{
		// 记录已经收敛的个数
		Map, num
	};

	public static class InputMapper extends Mapper<Object, Text, IntWritable, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			if(value.toString().equals("")) 
				return;
			for(int i = 1; i <= 6 ; i++) {
				context.write(new IntWritable(i), value);
			}
		}
	}
	
	public static class InputReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
			//parse input
			TreeMap<Integer, String> sort_container = new TreeMap<>();
			for(Text val : values) {
				String[] kv = val.toString().split("\\s+");
				sort_container.put(Integer.parseInt(kv[0]), kv[1]);
			}
			
			String res = "";
			for(String val : sort_container.values()) {
				res += val + " ";
			}
			
			context.write(key, new Text(res));
		}
	}
	
	public static class PageRankMapper extends Mapper<Object, Text, IntWritable, FloatWritable> {
		// 存储网页ID
		private IntWritable id;
		// 存储网页PR值
		private String pr;
		// 存储网页向外链接总数目
		private int count;
		// 网页向每个外部链接的平均贡献值
		private float average_pr;
		
		//转移矩阵
		private float[][] trans_matrix = {
				{0,    0, 0.333f, 0,   0,     0},
				{0.5f, 0, 0.333f, 0,   0,     0},
				{0.5f, 0, 0,      0,   0,     0},
				{0,    0, 0,      0,   0.5f,  1.0f},
				{0,    0, 0.333f, 0.5f, 0,    0},
				{0,    0, 0,      0.5f, 0.5f, 0}
		};
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				if(value.toString().equals(""))
					return;
				String[] strings = value.toString().split("\\s+");
				int map_key = Integer.parseInt(strings[0]);
				for(int i = 1; i <= 6; i++) {
					float mul_res = trans_matrix[map_key-1][i-1] * Float.parseFloat(strings[i]);
					context.write(new IntWritable(map_key), new FloatWritable(mul_res));
				}
		}
	}

	public static class PageRankReducer extends Reducer<IntWritable, FloatWritable, IntWritable, FloatWritable> {
		public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context) {
			float sum = 0;
			for(FloatWritable val : values) {
				sum +=val.get();
				System.out.print(String.valueOf(val));
			}
			System.out.println();
			//抽税法
			sum = 0.8f * sum + 1.0f / 6.0f * 0.2f;
			
			//面向主题
//			if(key.get() == 2 || key.get() == 5)
//				sum = 0.8f * sum + 1.0f / 2.0f * 0.2f;
//			else 
//				sum = 0.8f * sum;
			
			try {
				context.write(key, new FloatWritable(sum));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args)
			throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		Configuration conf = new Configuration();
		String pathIn1 = "input/PageRank/1"; // 输入路径
		String pathIn2 = "input/PageRank/2"; //转换输入格式
		String pathOut1 = "output/PageRank/1"; //输出路径，第二次迭代开始作为第一次mapreduce的输入路径
		
		// 迭代10次
		for (int i = 0; i < 20; i++) {
			{
				//转换输入格式
				Job job = Job.getInstance(conf, "PageRank");
				job.setJarByClass(PageRank.class);
				job.setMapperClass(InputMapper.class);
				job.setReducerClass(InputReducer.class);
				job.setOutputKeyClass(IntWritable.class);
				job.setOutputValueClass(Text.class);

				// 清空非初始输入路径
				if (i == 0) {
					FileSystem.get(job.getConfiguration()).delete(new Path(pathOut1), true);
					FileSystem.get(job.getConfiguration()).delete(new Path(pathIn2), true);
				}

				if (i == 0) {
					FileInputFormat.addInputPath(job, new Path(pathIn1));
					FileOutputFormat.setOutputPath(job, new Path(pathIn2));
				} else {
					FileSystem.get(job.getConfiguration()).delete(new Path(pathIn2), true);
					FileInputFormat.addInputPath(job, new Path(pathOut1));
					FileOutputFormat.setOutputPath(job, new Path(pathIn2));
				}
				job.waitForCompletion(true);
			}
			{
				//PageRank
				Job job = Job.getInstance(conf, "PageRank");
				job.setJarByClass(PageRank.class);
				job.setMapperClass(PageRankMapper.class);
				job.setReducerClass(PageRankReducer.class);
				job.setOutputKeyClass(IntWritable.class);
				job.setOutputValueClass(FloatWritable.class);
				FileSystem.get(job.getConfiguration()).delete(new Path(pathOut1), true);
				FileInputFormat.addInputPath(job, new Path(pathIn2));
				FileOutputFormat.setOutputPath(job, new Path(pathOut1));
				job.waitForCompletion(true);
			}
		}
	}
}
