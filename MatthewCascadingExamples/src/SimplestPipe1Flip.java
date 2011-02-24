import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.TextDelimited;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 * User: mccm06
 * Date: 2/23/11
 * Time: 7:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class SimplestPipe1Flip {
    public static void main(String[] args) {

        String inputPath = "data/babynamedefinitions.csv";
        String outputPath = "output/simplestpipe1";

        // define source and sink Taps.
        Scheme sourceScheme = new TextDelimited( new Fields( "name", "definition" ), "," );
        Tap source = new Hfs( sourceScheme, inputPath );

        Scheme sinkScheme = new TextDelimited( new Fields( "definition", "name" ), " ++ " );
        Tap sink = new Hfs( sinkScheme, outputPath, SinkMode.REPLACE );

        // the 'head' of the pipe assembly
        Pipe assembly = new Pipe( "flip" );
        //assembly = new Each( assembly, DebugLevel.VERBOSE, new Debug() );

        // initialize app properties, tell Hadoop which jar file to use
        Properties properties = new Properties();
        FlowConnector.setApplicationJarClass(properties, SimplestPipe1Flip.class);

        // plan a new Flow from the assembly using the source and sink Taps
        // with the above properties
        FlowConnector flowConnector = new FlowConnector( properties );
        //FlowConnector.setDebugLevel( properties, DebugLevel.VERBOSE );
        Flow flow = flowConnector.connect( "flipflow", source, sink, assembly );

        // execute the flow, block until complete
        flow.complete();
    }


}
