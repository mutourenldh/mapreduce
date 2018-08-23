package com.haoge.mapreduce.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 输入的时候key为字节数，value为每行数据 输出的时候key为手机号码，value为FlowBean对象
 * 
 * @author LDH
 *
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

	Text k = new Text();
	FlowBean v = new FlowBean();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 1.读取数据
		String line = value.toString();

		// 2.切割数据
		String[] fields = line.split("\t");

		// 3. 封装数据
		String phone = fields[0];
		long upFlow = Long.parseLong(fields[1]);
		long downFlow = Long.parseLong(fields[2]);
		v.set(upFlow, downFlow);
		k.set(phone);
		context.write(k, v);

	}
}
