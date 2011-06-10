import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.TextDelimited;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import java.util.Properties;

/**
 * Parse the input (CSV) and output it with ++ as the separator
 */
public class SimplestPipe4WordCount {
    public static void main(String[] args) {
        String inputPath = "data/babynamedefinitions.csv";
        String outputPath = "output/simplestpipe4";

        Scheme sourceScheme = new TextDelimited( new Fields( "name", "definition" ), "," );
        Tap source = new Hfs( sourceScheme, inputPath );

        Scheme sinkScheme = new TextDelimited( new Fields( "word", "count" ), " ^^ " );
        Tap sink = new Hfs( sinkScheme, outputPath, SinkMode.REPLACE );

        Pipe assembly = new Pipe( "flip" );

        RegexGenerator wordGenerator = new RegexGenerator( new Fields( "word" ), "(\\w)+" );
        assembly = new Each( assembly, new Fields( "definition" ), wordGenerator, new Fields("word") );

        assembly = new GroupBy(assembly, new Fields( "word" ) );
        assembly = new Every( assembly, new Fields( "word" ), new Count(), new Fields( "word", "count" ) );

        //OPTIONAL: Debug the tuple
        assembly = new Each( assembly, DebugLevel.VERBOSE, new Debug() );

        Properties properties = new Properties();
        FlowConnector.setApplicationJarClass(properties, SimplestPipe4WordCount.class);

        FlowConnector flowConnector = new FlowConnector( properties );
        //OPTIONAL: Have the planner use or filter out the debugging statements
        FlowConnector.setDebugLevel( properties, DebugLevel.VERBOSE );
        Flow flow = flowConnector.connect( "flipflow", source, sink, assembly );
        flow.complete();
    }
}
