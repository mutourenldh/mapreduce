package com.haoge.mapreduce.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowCountReduce extends Reducer<Text, FlowBean, Text, FlowBean> {

	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context)
			throws IOException, InterruptedException {

		long sumUpFlow = 0;
		long sumDownFlow = 0;

		// 循环每个键值对，对flowbean中的upflow和downflow进行求和
		for (FlowBean flowBean : values) {
			sumUpFlow += flowBean.getUpFlow();
			sumDownFlow += flowBean.getDownFlow();

		}
		// 封装数据
		FlowBean flowBean = new FlowBean(sumUpFlow, sumDownFlow);
		// 写出数据
		context.write(key, flowBean);

	}

}
