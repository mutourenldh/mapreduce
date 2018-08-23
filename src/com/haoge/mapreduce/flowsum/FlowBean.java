package com.haoge.mapreduce.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

//1.必须实现 Writable 接口
public class FlowBean implements Writable {

	private long upFlow;// 上行流量
	private long downFlow;// 下行流量
	private long sumFlow;// 总流量

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	// 3.重写序列化方法
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}

	// 4. 重写反序列化方法
	// 5.注意反序列化的顺序和序列化的顺序完全一致
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upFlow = in.readLong();
		this.downFlow = in.readLong();
		this.sumFlow = in.readLong();
	}

	// 2.反序列化时，需要反射调用空参构造函数，所以必须有空参构造
	public FlowBean() {
		super();
		// TODO Auto-generated constructor stub
	}

	// 6.要想把结果显示在文件中，需要重写 toString()，可用”\t”分开，方便后续用。
	@Override
	public String toString() {
		return upFlow + "\t" + downFlow + "\t" + sumFlow;
	}

	public FlowBean(long upFlow, long downFlow) {
		super();
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}

	public void set(long upflow, long downflow) {
		this.upFlow = upflow;
		this.downFlow = downflow;
		this.sumFlow = upflow + downflow;

	}
	// 7. 如果需要将自定义的 bean 放在 key 中传输，则还需要实现 comparable 接口，因为
	// mapreduce 框中的 shuffle 过程一定会对 key 进行排序。

}
