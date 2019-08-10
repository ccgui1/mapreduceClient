package com.ccgui.wordcount;

import java.io.IOException;

import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.NewEpochRequestProto;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.sun.tools.classfile.StackMapTable_attribute.verification_type_info;

/*
	  1��Mapper �׶�
	��1���û��Զ���� Mapper Ҫ�̳��Լ��ĸ���
	��2��Mapper ������������ KV �Ե���ʽ��KV �����Ϳ��Զ��壩
	��3��Mapper �е�ҵ���߼�д�� map()������
	��4��Mapper ����������� KV �Ե���ʽ��KV �����Ϳ��Զ��壩
	��5��map()������maptask ���̣���ÿһ��<K,V>����һ��
*/
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	Text k = new Text();
	IntWritable v = new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//1.��һ������ת��Ϊstring����
		String line = value.toString();
		//2.�и�
		String[] words = line.split("[ ;.,]");
		//3.ѭ��
		for (String word : words){
			k.set(word);
			context.write(k, v);
		}
	}
}
