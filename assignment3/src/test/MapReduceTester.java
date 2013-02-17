import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.*;
import edu.umd.cloud9.io.pair.PairOfInts;


public class MapReduceTester {
    @Test
    public void processesValidRecord() throws IOException, InterruptedException {
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+038299999V0203201N00261220001CN9999999N9-00111+99999999999");

        new MapDriver<LongWritable, Text, Text, PairOfInts>()
            .withMapper(new InvertedIndexMapper())
            .withInputValue(value)
            .withOutput(new Text("1950"), new PairOfInts(-11, 10))
            .runTest();
    }
    
    
    @Test
    public void returnsMaximumIntegerInValues() throws IOException,
    InterruptedException {
        new ReduceDriver<Text, PairOfInts, Text, Text>()
            .withReducer(new InvertedIndexReducer())
            .withInputKey(new Text("1950"))
            .withInputValues(Arrays.asList(new IntWritable(10), new IntWritable(5)))
            .withOutput(new Text("1950"), new IntWritable(10))
            .runTest();
    }
}