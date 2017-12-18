import java.io.IOException;
import java.util.StringTokenizer;

//hadoop library
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndex{
	
	//this is Mapper class
	public static class InvertedIndexMapper extends
		Mapper<Object,Text,Object,Text>{
			private Text keyInfo = new Text();//save word and path
			private Text count = new Text();
			private FileSplit split;//save Split's object
			@Override
			public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
					//getcurrent file
					split = (FileSplit)context.getInputSplit();
					
					/*use StringTokenizer to divided every line
					into many single words*/
					StringTokenizer itr = new StringTokenizer(value.toString());
					while(itr.hasMoreTokens()){
						//getPath() return the path of current file
						keyInfo.set(itr.nextToken()+":"+split.getPath().toString());
						count.set("1");
						//output as <key, value>
						//such as <"hdfs:license.txt",1>
						context.write(keyInfo, count);
					}
					
				}
		}

	//this is Combiner class
	public static class InvertedIndexCombiner
		extends Reducer<Text,Text,Text,Text>{
			private Text info = new Text();//use to store information
			@Override
			protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
					//input form: <"hdfs:license.txt",list(1,1,1,1)>
					//output form:<"name",".txt:2">
					int sum = 0;
					for (Text value : values){
						sum += Integer.parseInt(value.toString());
					}
					//find the position of ":"
					int splitIndex = key.toString().indexOf(":");
					info.set(key.toString().substring(splitIndex+1)+":"+sum);
					key.set(key.toString().substring(0,splitIndex));
					//output as <"hdfs","license:9">
					context.write(key,info);
				}
		}
		
		
	//this is Reducer class
	public static class InvertedIndexReducer
		extends Reducer<Text, Text, Text, Text>{
			//<key,result>
			private Text result = new Text();
			
			@Override
			protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
					//input:<"name", list(".text:2",".txt:3",...)>
					//output:<"name",".txt:2,.txt:3,...">
					
					String fileList = new String();
					for (Text value : values){
						fileList += value.toString()+";";
					}
					result.set(fileList);
					context.write(key,result);
				}
		}
		
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: InvertedIndex <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "InvertedIndex");
	    job.setJarByClass(InvertedIndex.class);
		//use InvertedIndexMapper class to map
	    job.setMapperClass(InvertedIndexMapper.class);
		//use InvertedIndexCombiner class to combine
	    job.setCombinerClass(InvertedIndexCombiner.class);
		//use InvertedIndexReducer class to reduce
	    job.setReducerClass(InvertedIndexReducer.class);
		//set the output format of Map process and Reduce Produce
	    job.setOutputKeyClass(Text.class);
		//set the format of value in the output of Map and Produce process
	    job.setOutputValueClass(Text.class);
		
		
		// 判断output文件夹是否存在，如果存在则删除  
		/*Path path = new Path(otherArgs[1]);// 取第1个表示输出目录参数（第0个参数是输入目录）  
		FileSystem fileSystem = path.getFileSystem(conf);// 根据path找到这个文件  
		if (fileSystem.exists(path)) {  
			fileSystem.delete(path, true);// true的意思是，就算output有东西，也一带删除  
		}  */
		
		
		//set the input path of data
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		//set the output's saving path
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//call job.waitForCompletion(true) to run the task and exit after finishing
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
