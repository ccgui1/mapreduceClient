package com.ccgui.wordcount;

import java.io.IOException;

import org.apache.commons.math3.stat.descriptive.summary.Sum;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.sun.tools.classfile.StackMapTable_attribute.verification_type_info;

/*2．Reducer 阶段
（1）用户自定义的 Reducer 要继承自己的父类
（2）Reducer 的输入数据类型对应 Mapper 的输出数据类型，也是 KV
（3）Reducer 的业务逻辑写在 reduce()方法中
（4）Reducetask 进程对每一组相同 k 的<k,v>组调用一次 reduce()方法*/

public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
int sum;
IntWritable v =  new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			Reducer<Text, IntWritable, Text, IntWritable>.Context comtext) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	
		
		//1. 累加求和
		sum = 0;
		for (IntWritable count : value){
			sum += count.get();
		}
		//2. 输出
		v.set(sum);
		comtext.write(key, v);
	}
}
