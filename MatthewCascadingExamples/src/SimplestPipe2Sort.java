import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.TextDelimited;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by IntelliJ IDEA.
 * User: mccm06
 * Date: 2/23/11
 * Time: 7:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class SimplestPipe2Sort {
    public static void main(String[] args) {

        String inputPath = "data/babynamedefinitions.csv";
        String outputPath = "output/simplestpipe2";

        // define source and sink Taps.
        Scheme sourceScheme = new TextDelimited( new Fields( "name", "definition" ), "," );
        Tap source = new Hfs( sourceScheme, inputPath );

        Scheme sinkScheme = new TextDelimited( new Fields( "definition", "name" ), " ^^^ " );
        Tap sink = new Hfs( sinkScheme, outputPath, SinkMode.REPLACE );


        // the 'head' of the pipe assembly
        Pipe assembly = new Pipe( "sortreverse" );
        Fields groupFields = new Fields( "name");
        groupFields.setComparator("name", Collections.reverseOrder());

        assembly = new GroupBy( assembly, groupFields );


        // initialize app properties, tell Hadoop which jar file to use
        Properties properties = new Properties();
        FlowConnector.setApplicationJarClass(properties, SimplestPipe2Sort.class);

        // plan a new Flow from the assembly using the source and sink Taps
        // with the above properties
        FlowConnector flowConnector = new FlowConnector( properties );
        Flow flow = flowConnector.connect( "sortflow", source, sink, assembly );

        // execute the flow, block until complete
        flow.complete();
    }


}
