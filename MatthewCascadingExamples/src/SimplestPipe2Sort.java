import cascading.CascadingException;
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
 * Sort the output in reverse order by name
 */
public class SimplestPipe2Sort {
    public static void main(String[] args) {
        String inputPath = "data/babynamedefinitions.csv";
        String outputPath = "output/simplestpipe2";

        Scheme sourceScheme = new TextDelimited( new Fields( "name", "definition" ), "," );
        Tap source = new Hfs( sourceScheme, inputPath );

        Scheme sinkScheme = new TextDelimited( new Fields( "definition", "name" ), "++" );
        Tap sink = new Hfs( sinkScheme, outputPath, SinkMode.REPLACE );


        Pipe assembly = new Pipe( "sortreverse" );
        Fields groupFields = new Fields( "name");
        //OPTIONAL: Set the comparator
        groupFields.setComparator("name", Collections.reverseOrder());

        assembly = new GroupBy( assembly, groupFields );


        Properties properties = new Properties();
        FlowConnector.setApplicationJarClass(properties, SimplestPipe2Sort.class);

        FlowConnector flowConnector = new FlowConnector( properties );
        Flow flow = flowConnector.connect( "sortflow", source, sink, assembly );
        flow.complete();

        //OPTIONAL: Output a debugging diagram
        flow.writeDOT(outputPath + "/flowdiagram.dot");
    }
}
