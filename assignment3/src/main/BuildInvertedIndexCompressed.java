import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cern.colt.Arrays;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfStringInt;
import edu.umd.cloud9.io.pair.PairOfWritables;
import edu.umd.cloud9.util.fd.Object2IntFrequencyDistribution;
import edu.umd.cloud9.util.fd.Object2IntFrequencyDistributionEntry;
import edu.umd.cloud9.util.pair.PairOfObjectInt;

public class BuildInvertedIndexCompressed extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

    // Mapper: emits (term, tf) for every word in the document.
    private static class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, VIntWritable> {
        
        private static final Object2IntFrequencyDistribution<String> COUNTS =
                new Object2IntFrequencyDistributionEntry<String>();
        private static final PairOfStringInt KEY = new PairOfStringInt();
        private static final VIntWritable VALUE = new VIntWritable();

        @Override
        public void map(LongWritable docno, Text doc, Context context)
                throws IOException, InterruptedException {

            String line = ((Text) doc).toString();
            StringTokenizer itr = new StringTokenizer(line);
            COUNTS.clear();
            String term;

            // Build a histogram of the terms.
            while (itr.hasMoreTokens()) {
                term = itr.nextToken();
                if (term == null || term.length() == 0) {
                    continue;
                }

                COUNTS.increment(term);
            }
            
            // Emit postings of the form - ((term, docID), tf)
            for (PairOfObjectInt<String> e : COUNTS) {
                
                KEY.set(e.getLeftElement(), (int)docno.get());
                VALUE.set(e.getRightElement());
                context.write(KEY,  VALUE);
            }
        }
    }

    protected static class MyPartitioner extends Partitioner<PairOfStringInt, VIntWritable> {
        @Override
        public int getPartition(PairOfStringInt key, VIntWritable value, int numReduceTasks) {
            return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    private static class MyReducer extends
    Reducer<PairOfStringInt, VIntWritable, Text, BytesWritable> {
        
        private static final Text KEY = new Text();
        private static final BytesWritable POSTINGS = new BytesWritable();
        private static String prevTerm = "";
        private static String term = "";
        private static int prevDocID = 0;
        
        private static final ByteArrayOutputStream out = new ByteArrayOutputStream();
        private static final DataOutputStream dataOut = new DataOutputStream(out);
        
        @Override
        public void reduce(PairOfStringInt key, Iterable<VIntWritable> values, Context context)
                throws IOException, InterruptedException {
            
            Iterator<VIntWritable> iter = values.iterator();
            int tf = 0;;
            if (iter.hasNext()) { tf = iter.next().get(); }
            
            term = key.getLeftElement();
            if (term.compareTo(prevTerm) != 0 && !prevTerm.isEmpty()) {
                KEY.set(prevTerm);
                prevDocID = 0;
                
                POSTINGS.set(out.toByteArray(), 0, out.size());
                context.write(KEY,  POSTINGS);
                
                // clear the streams
                out.flush();
                out.reset();
                dataOut.flush();
            }
            
            // use d-gap compression on the docID
            WritableUtils.writeVInt(dataOut, (key.getRightElement()-prevDocID));
            WritableUtils.writeVInt(dataOut, tf);
            
            prevDocID = key.getRightElement();
            prevTerm = term;
        }
        
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(KEY, POSTINGS);
        }
    }
    
    
    /**
     * Creates an instance of this tool.
     */
    public BuildInvertedIndexCompressed() {}

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";

    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of reducers").create(NUM_REDUCERS));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

        LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
        LOG.info(" - input path: " + inputPath);
        LOG.info(" - output path: " + outputPath);
        LOG.info(" - number of reducers: " + reduceTasks);

        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJobName(BuildInvertedIndexCompressed.class.toString());
        job.setJarByClass(BuildInvertedIndexCompressed.class);
        job.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setOutputFormatClass(MapFileOutputFormat.class);

        job.setMapOutputKeyClass(PairOfStringInt.class);
        job.setMapOutputValueClass(VIntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setPartitionerClass(MyPartitioner.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(outputPath);
        FileSystem.get(conf).delete(outputDir, true);
        
        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new BuildInvertedIndexCompressed(), args);
    }
}