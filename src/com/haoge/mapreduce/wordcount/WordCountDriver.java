package com.haoge.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 统计一堆文件中每个单词出现的个数
 * @author LDH
 *
 */
public class WordCountDriver {

	public static void main(String[] args) throws Exception {
		// 1.获取配置信息
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		// 2.设置jar加载路径
		job.setJarByClass(WordCountDriver.class);

		// 3.设置mapper和reduce类
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReduce.class);

		// 4.设置mapper输出
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		// 5.设置reduce输出
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 6. 设置输入输出路径 输入路径为d:/input/wordcount	输出路径为d:/output
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 7. 提交
		boolean result = job.waitForCompletion(true);

		System.exit(result ? 0 : 1);

	}

}
