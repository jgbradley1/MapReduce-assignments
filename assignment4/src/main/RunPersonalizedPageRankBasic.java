import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.mapreduce.lib.input.NonSplitableSequenceFileInputFormat;

public class RunPersonalizedPageRankBasic extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(RunPersonalizedPageRankBasic.class);

    private static enum PageRank {
        nodes, edges, massMessages, massMessagesSaved, massMessagesReceived, missingStructure
    };

    private static class MyMapper extends
    Mapper<IntWritable, PersonalizedPageRankNode, IntWritable, PersonalizedPageRankNode> {

        // The neighbor to which we're sending messages.
        private static final IntWritable neighbor = new IntWritable();

        // Contents of the messages: partial PageRank mass.
        private static final PersonalizedPageRankNode intermediateMass = new PersonalizedPageRankNode();

        // For passing along node structure.
        private static final PersonalizedPageRankNode intermediateStructure = new PersonalizedPageRankNode();

        // For holding the nodeID of all sources
        private static ArrayList<Integer> sources;

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();

            String[] sourceList = conf.getStrings(SOURCES);
            sources = new ArrayList<Integer>(sourceList.length);
            for (int i=0; i<sourceList.length; i++) {
                sources.add(i, Integer.parseInt(sourceList[i]));
            }
        }


        @Override
        public void map(IntWritable nid, PersonalizedPageRankNode node, Context context)
                throws IOException, InterruptedException {
            // Pass along node structure.
            intermediateStructure.setNodeId(node.getNodeId());
            intermediateStructure.setType(PersonalizedPageRankNode.Type.Structure);
            intermediateStructure.setAdjacencyList(node.getAdjacenyList());

            context.write(nid, intermediateStructure);

            int massMessages = 0;

            // Distribute PageRank mass to neighbors (along outgoing edges).
            if (node.getAdjacenyList().size() > 0) {
                // Each neighbor gets an equal share of PageRank mass.
                ArrayListOfIntsWritable list = node.getAdjacenyList();
                float mass[] = new float[sources.size()];
                for (int i = 0; i<sources.size(); i++) {
                    mass[i] = node.getPageRank(i) - (float) StrictMath.log(list.size());
                }

                context.getCounter(PageRank.edges).increment(list.size());

                // Iterate over neighbors.
                for (int i = 0; i < list.size(); i++) {
                    neighbor.set(list.get(i));
                    intermediateMass.setNodeId(list.get(i));
                    intermediateMass.setType(PersonalizedPageRankNode.Type.Mass);

                    for (int j=0; j<sources.size(); j++) {
                        intermediateMass.setPageRank(j, mass[j]);
                    }

                    // Emit messages with PageRank mass to neighbors.
                    context.write(neighbor, intermediateMass);

                    massMessages++;
                }
            }

            // Bookkeeping.
            context.getCounter(PageRank.nodes).increment(1);
            context.getCounter(PageRank.massMessages).increment(massMessages);
        }
    }

    // Combiner: sums partial PageRank contributions and passes node structure along.
    private static class MyCombiner extends
    Reducer<IntWritable, PersonalizedPageRankNode, IntWritable, PersonalizedPageRankNode> {
        private static final PersonalizedPageRankNode intermediateMass = new PersonalizedPageRankNode();

        // For holding the nodeID of all sources
        private static ArrayList<Integer> sources;

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();

            String[] sourceList = conf.getStrings(SOURCES);
            sources = new ArrayList<Integer>(sourceList.length);
            for (int i=0; i<sourceList.length; i++) {
                sources.add(i, Integer.parseInt(sourceList[i]));
            }
        }

        @Override
        public void reduce(IntWritable nid, Iterable<PersonalizedPageRankNode> values, Context context)
                throws IOException, InterruptedException {
            int massMessages = 0;

            // Remember, PageRank mass is stored as a log prob.
            float[] mass = new float[sources.size()];
            for (int i=0; i<sources.size(); i++) {
                mass[i] = Float.NEGATIVE_INFINITY;
            }

            for (PersonalizedPageRankNode n : values) {
                if (n.getType() == PersonalizedPageRankNode.Type.Structure) {
                    // Simply pass along node structure.
                    context.write(nid, n);
                } else {
                    // Accumulate PageRank mass contributions.
                    for (int j=0; j<sources.size(); j++) {
                        mass[j] = sumLogProbs(mass[j], n.getPageRank(j));
                    }
                    massMessages++;
                }
            }

            // Emit aggregated results.
            if (massMessages > 0) {
                intermediateMass.setNodeId(nid.get());
                intermediateMass.setType(PersonalizedPageRankNode.Type.Mass);
                for (int i=0; i<sources.size(); i++) {
                    intermediateMass.setPageRank(i, mass[i]);
                }

                context.write(nid, intermediateMass);
            }
        }
    }

    // Reduce: sums incoming PageRank contributions, rewrite graph structure.
    private static class MyReducer extends
    Reducer<IntWritable, PersonalizedPageRankNode, IntWritable, PersonalizedPageRankNode> {
        // For keeping track of PageRank mass encountered, so we can compute missing PageRank mass lost
        // through dangling nodes.
        private float totalMass[];

        // For holding the nodeID of all sources
        private static ArrayList<Integer> sources;

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();

            String[] sourceList = conf.getStrings(SOURCES);
            sources = new ArrayList<Integer>(sourceList.length);
            for (int i=0; i<sourceList.length; i++) {
                sources.add(i, Integer.parseInt(sourceList[i]));
            }
            
            totalMass = new float[sources.size()];
            for (int i=0; i<sourceList.length; i++) {
                totalMass[i] = Float.NEGATIVE_INFINITY;
            }
        }

        @Override
        public void reduce(IntWritable nid, Iterable<PersonalizedPageRankNode> iterable, Context context)
                throws IOException, InterruptedException {
            Iterator<PersonalizedPageRankNode> values = iterable.iterator();

            // Create the node structure that we're going to assemble back together from shuffled pieces.
            PersonalizedPageRankNode node = new PersonalizedPageRankNode();

            node.setType(PersonalizedPageRankNode.Type.Complete);
            node.setNodeId(nid.get());

            int massMessagesReceived = 0;
            int structureReceived = 0;

            float[] mass = new float[sources.size()];
            for (int i=0; i<sources.size(); i++) {
                mass[i] = Float.NEGATIVE_INFINITY;
            }

            while (values.hasNext()) {
                PersonalizedPageRankNode n = values.next();

                if (n.getType().equals(PersonalizedPageRankNode.Type.Structure)) {
                    // This is the structure; update accordingly.
                    ArrayListOfIntsWritable list = n.getAdjacenyList();
                    structureReceived++;

                    node.setAdjacencyList(list);
                } else {
                    // This is a message that contains PageRank mass; accumulate.
                    for (int i=0; i<sources.size(); i++) {
                        mass[i] = sumLogProbs(mass[i], n.getPageRank(i));
                    }
                    massMessagesReceived++;
                }
            }

            // Update the final accumulated PageRank mass.
            for (int i=0; i<sources.size(); i++) {
                node.setPageRank(i, mass[i]);
            }
            context.getCounter(PageRank.massMessagesReceived).increment(massMessagesReceived);

            // Error checking.
            if (structureReceived == 1) {
                // Everything checks out, emit final node structure with updated PageRank value.
                context.write(nid, node);

                // Keep track of total PageRank mass.
                for (int i=0; i<sources.size(); i++) {
                    totalMass[i] = sumLogProbs(totalMass[i], mass[i]);
                }
            } else if (structureReceived == 0) {
                // We get into this situation if there exists an edge pointing to a node which has no
                // corresponding node structure (i.e., PageRank mass was passed to a non-existent node)...
                // log and count but move on.
                context.getCounter(PageRank.missingStructure).increment(1);
                LOG.warn("No structure received for nodeid: " + nid.get() + " mass: "
                        + massMessagesReceived);
                // It's important to note that we don't add the PageRank mass to total... if PageRank mass
                // was sent to a non-existent node, it should simply vanish.
            } else {
                // This shouldn't happen!
                throw new RuntimeException("Multiple structure received for nodeid: " + nid.get()
                        + " mass: " + massMessagesReceived + " struct: " + structureReceived);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String taskId = conf.get("mapred.task.id");
            String path = conf.get("PageRankMassPath");

            Preconditions.checkNotNull(taskId);
            Preconditions.checkNotNull(path);

            // Write to a file the amount of PageRank mass we've seen in this reducer.
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataOutputStream out = fs.create(new Path(path + "/" + taskId), false);
            for (int i=0; i<sources.size(); i++) {
                out.writeFloat(totalMass[i]);
            }
            out.close();
        }
    }

    // Mapper that distributes the missing PageRank mass (lost at the dangling nodes) and takes care
    // of the random jump factor.
    private static class MapPageRankMassDistributionClass extends
    Mapper<IntWritable, PersonalizedPageRankNode, IntWritable, PersonalizedPageRankNode> {
        private float[] missingMass;
        private ArrayList<Integer> sources;

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();

            String[] sourceList = conf.getStrings(SOURCES);
            sources = new ArrayList<Integer>(sourceList.length);
            for (int i=0; i<sourceList.length; i++) {
                sources.add(i, Integer.parseInt(sourceList[i]));
            }

            String[] missingValues = conf.getStrings("MissingMass");
            missingMass = new float[missingValues.length];
            for (int i=0; i<sources.size(); i++) {
                missingMass[i] = Float.parseFloat(missingValues[i]);
            }
        }

        @Override
        public void map(IntWritable nid, PersonalizedPageRankNode node, Context context)
                throws IOException, InterruptedException {
            
            for (int sourceIndex=0; sourceIndex<sources.size(); sourceIndex++) {
                float p = node.getPageRank(sourceIndex);

                float jump = 0.0f;
                float link = 0.0f;

                //float jump = (float) (Math.log(ALPHA) - Math.log(nodeCnt));
                // IS A SOURCE NODE
                if (sources.get(sourceIndex) == node.getNodeId()) {
                    jump = (float) Math.log(ALPHA);
                    link = (float) Math.log(1.0f - ALPHA) + sumLogProbs(p, (float) (Math.log(missingMass[sourceIndex])));
                }
                else {
                    // NOT A SOURCE NODE
                    jump = (float) Math.log(0);
                    link = (float) Math.log(1.0f - ALPHA) + p;
                }

                p = sumLogProbs(jump, link);
                node.setPageRank(sourceIndex, p);
            }

            context.write(nid, node);
        }
    }

    // Random jump factor.
    private static float ALPHA = 0.15f;
    private static NumberFormat formatter = new DecimalFormat("0000");

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new RunPersonalizedPageRankBasic(), args);
    }

    public RunPersonalizedPageRankBasic() {}

    private static final String BASE = "base";
    private static final String NUM_NODES = "numNodes";
    private static final String START = "start";
    private static final String END = "end";
    private static final String COMBINER = "useCombiner";
    private static final String INMAPPER_COMBINER = "useInMapperCombiner";
    private static final String RANGE = "range";
    private static final String SOURCES = "sources";
    private static Configuration conf;

    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(new Option(COMBINER, "use combiner"));
        options.addOption(new Option(INMAPPER_COMBINER, "user in-mapper combiner"));
        options.addOption(new Option(RANGE, "use range partitioner"));

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("base path").create(BASE));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("start iteration").create(START));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("end iteration").create(END));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of nodes").create(NUM_NODES));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("source nodes").create(SOURCES));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(BASE) || !cmdline.hasOption(START) ||
                !cmdline.hasOption(END) || !cmdline.hasOption(NUM_NODES) ||
                !cmdline.hasOption(SOURCES)) {

            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String basePath = cmdline.getOptionValue(BASE);
        int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
        int s = Integer.parseInt(cmdline.getOptionValue(START));
        int e = Integer.parseInt(cmdline.getOptionValue(END));
        String[] sources = cmdline.getOptionValues(SOURCES)[0].split(",");
        boolean useCombiner = cmdline.hasOption(COMBINER);
        boolean useInmapCombiner = cmdline.hasOption(INMAPPER_COMBINER);
        boolean useRange = cmdline.hasOption(RANGE);

        LOG.info("Tool name: RunPageRank");
        LOG.info(" - base path: " + basePath);
        LOG.info(" - num nodes: " + n);
        LOG.info(" - start iteration: " + s);
        LOG.info(" - end iteration: " + e);
        LOG.info(" - use combiner: " + useCombiner);
        LOG.info(" - use in-mapper combiner: " + useInmapCombiner);
        LOG.info(" - user range partitioner: " + useRange);

        conf = getConf();
        conf.setStrings(SOURCES, sources);

        // Iterate PageRank.
        for (int i = s; i < e; i++) {
            iteratePageRank(i, i + 1, basePath, n, useCombiner, useInmapCombiner, sources.length);
        }

        return 0;
    }

    // Run each iteration.
    private void iteratePageRank(int i, int j, String basePath, int numNodes,
            boolean useCombiner, boolean useInMapperCombiner, int numberOfSources) throws Exception {
        // Each iteration consists of two phases (two MapReduce jobs).

        // Job 1: distribute PageRank mass along outgoing edges.
        float mass[] = phase1(i, j, basePath, numNodes, useCombiner, useInMapperCombiner, numberOfSources);

        // Find out how much PageRank mass got lost at the dangling nodes.
        float[] missing = new float[numberOfSources];
        for (int k=0; k<numberOfSources; k++) {
            missing[k] = 1.0f - (float) StrictMath.exp(mass[k]);
        }

        // Job 2: distribute missing mass, take care of random jump factor.
        phase2(i, j, missing, basePath, numNodes);
    }

    private float[] phase1(int i, int j, String basePath, int numNodes,
            boolean useCombiner, boolean useInMapperCombiner, int numSources) throws Exception {
        Job job = Job.getInstance(conf);
        job.setJobName("PageRank:Basic:iteration" + j + ":Phase1");
        job.setJarByClass(RunPersonalizedPageRankBasic.class);

        String in = basePath + "/iter" + formatter.format(i);
        String out = basePath + "/iter" + formatter.format(j) + "t";
        String outm = out + "-mass";

        // We need to actually count the number of part files to get the number of partitions (because
        // the directory might contain _log).
        int numPartitions = 0;
        for (FileStatus s : FileSystem.get(conf).listStatus(new Path(in))) {
            if (s.getPath().getName().contains("part-"))
                numPartitions++;
        }

        LOG.info("PageRank: iteration " + j + ": Phase1");
        LOG.info(" - input: " + in);
        LOG.info(" - output: " + out);
        LOG.info(" - nodeCnt: " + numNodes);
        LOG.info(" - useCombiner: " + useCombiner);
        LOG.info(" - useInmapCombiner: " + useInMapperCombiner);
        LOG.info("computed number of partitions: " + numPartitions);

        int numReduceTasks = numPartitions;

        job.getConfiguration().setInt("NodeCount", numNodes);
        job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
        job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
        job.getConfiguration().set("PageRankMassPath", outm);

        job.setNumReduceTasks(numReduceTasks);

        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PersonalizedPageRankNode.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PersonalizedPageRankNode.class);

        job.setMapperClass(MyMapper.class);

        if (useCombiner) {
            job.setCombinerClass(MyCombiner.class);
        }

        job.setReducerClass(MyReducer.class);

        FileSystem.get(conf).delete(new Path(out), true);
        FileSystem.get(conf).delete(new Path(outm), true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        float mass[] = new float[numSources];
        for (int k = 0; k<numSources; k++) {
            mass[k] = Float.NEGATIVE_INFINITY;
        }

        FileSystem fs = FileSystem.get(conf);
        for (FileStatus f : fs.listStatus(new Path(outm))) {
            FSDataInputStream fin = fs.open(f.getPath());
            for (int k = 0; k<numSources; k++) {
                mass[k] = sumLogProbs(mass[k], fin.readFloat());
            }
            fin.close();
        }

        return mass;
    }

    private void phase2(int i, int j, float[] missing, String basePath, int numNodes) throws Exception {
        Job job = Job.getInstance(conf);
        job.setJobName("PageRank:Basic:iteration" + j + ":Phase2");
        job.setJarByClass(RunPersonalizedPageRankBasic.class);

        LOG.info("missing PageRank mass: " + missing);
        LOG.info("number of nodes: " + numNodes);

        String in = basePath + "/iter" + formatter.format(j) + "t";
        String out = basePath + "/iter" + formatter.format(j);

        LOG.info("PageRank: iteration " + j + ": Phase2");
        LOG.info(" - input: " + in);
        LOG.info(" - output: " + out);

        job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
        job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
        job.getConfiguration().setInt("NodeCount", numNodes);

        String[] missingValues = new String[missing.length];
        for (int k=0; k<missing.length; k++) {
            missingValues[k] = Float.toString(missing[k]);
        }
        job.getConfiguration().setStrings("MissingMass", missingValues);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PersonalizedPageRankNode.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PersonalizedPageRankNode.class);

        job.setMapperClass(MapPageRankMassDistributionClass.class);

        FileSystem.get(conf).delete(new Path(out), true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }

    // Adds two log probs.
    private static float sumLogProbs(float a, float b) {
        if (a == Float.NEGATIVE_INFINITY)
            return b;

        if (b == Float.NEGATIVE_INFINITY)
            return a;

        if (a < b) {
            return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
        }

        return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
    }
}
