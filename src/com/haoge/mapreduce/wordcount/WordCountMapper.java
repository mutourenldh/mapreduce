package com.haoge.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 泛型分别定义mapper阶段输入的key，value类型，输出阶段的key，value
 * 
 * @author LDH
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	// 原理：将文件中的每行数据独读出，以空格切割，mapper阶段输出格式为<key,1>
	// 然后在reduce阶段 key相同的进入一个reduce,汇总每个key的个数
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		IntWritable v = new IntWritable(1);
		Text k = new Text();
		// 1.读取一行
		String line = value.toString();
		// 2.以空格切割
		String[] words = line.split(" ");
		// 3.输出
		for (String word : words) {
			k.set(word);
			context.write(k, v);

		}
	}
}
