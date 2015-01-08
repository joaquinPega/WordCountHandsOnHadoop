package example.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text,Text,LongWritable> {
	private static final LongWritable ONE = new LongWritable(1);

	@Override
	protected void map(LongWritable key, Text value,Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString().toLowerCase();
		String[] words = line.split(" ");
		for (String word : words){
			context.write(new Text(word), ONE);
		}
	}
	
	
}
