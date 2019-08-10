package com.ccgui.wordcount;

import java.io.IOException;

import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochRequestProto;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.sun.tools.classfile.StackMapTable_attribute.verification_type_info;

/*
	  1．Mapper 阶段
	（1）用户自定义的 Mapper 要继承自己的父类
	（2）Mapper 的输入数据是 KV 对的形式（KV 的类型可自定义）
	（3）Mapper 中的业务逻辑写在 map()方法中
	（4）Mapper 的输出数据是 KV 对的形式（KV 的类型可自定义）
	（5）map()方法（maptask 进程）对每一个<K,V>调用一次
*/
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	Text k = new Text();
	IntWritable v = new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//1.将一行数据转化为string类型
		String line = value.toString();
		//2.切割
		String[] words = line.split("[ ;.,]");
		//3.循环
		for (String word : words){
			k.set(word);
			context.write(k, v);
		}
	}
}
