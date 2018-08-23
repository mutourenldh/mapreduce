package com.haoge.mapreduce.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowCountDriver {

	public static void main(String[] args) {

		try {
			// 1.获取配置信息，或者job对象实例
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf);

			// 2.设置jar加载路径
			job.setJarByClass(FlowCountDriver.class);

			// 3.设置mapper类和reduce类
			job.setMapperClass(FlowCountMapper.class);
			job.setReducerClass(FlowCountReduce.class);

			// 4.设置mapper输出
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(FlowBean.class);

			// 5. 设置reduce输出
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FlowBean.class);

			// 6.设置输入输出路径,输入路径为d:/input/flowcount	输出路径为d:/output
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			// 提交
			boolean result = job.waitForCompletion(true);

			System.exit(result ? 0 : 1);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
