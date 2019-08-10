package com.ccgui.wordcount;

import java.io.IOException;

import org.apache.commons.math3.stat.descriptive.summary.Sum;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.sun.tools.classfile.StackMapTable_attribute.verification_type_info;

/*2��Reducer �׶�
��1���û��Զ���� Reducer Ҫ�̳��Լ��ĸ���
��2��Reducer �������������Ͷ�Ӧ Mapper ������������ͣ�Ҳ�� KV
��3��Reducer ��ҵ���߼�д�� reduce()������
��4��Reducetask ���̶�ÿһ����ͬ k ��<k,v>�����һ�� reduce()����*/

public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
int sum;
IntWritable v =  new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			Reducer<Text, IntWritable, Text, IntWritable>.Context comtext) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
	
		
		//1. �ۼ����
		sum = 0;
		for (IntWritable count : value){
			sum += count.get();
		}
		//2. ���
		v.set(sum);
		comtext.write(key, v);
	}
}
