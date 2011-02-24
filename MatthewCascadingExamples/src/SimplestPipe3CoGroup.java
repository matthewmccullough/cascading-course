import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.CoGroup;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.cogroup.InnerJoin;
import cascading.scheme.Scheme;
import cascading.scheme.TextDelimited;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Take two data streams and combine them. One has names and descriptions of those names.
 * The other has names and birth counts with those names.
 */
public class SimplestPipe3CoGroup {
    public static void main(String[] args) {
        String inputPathDefinitions = "data/babynamedefinitions.csv";
        String inputPathCounts = "data/babynamecounts.csv";
        String outputPath = "output/simplestpipe3";

        Scheme sourceSchemeDefinitions = new TextDelimited( new Fields( "name", "definition" ), "," );
        Scheme sourceSchemeCounts = new TextDelimited( new Fields( "name", "count" ), "," );

        Tap sourceDefinitions = new Hfs( sourceSchemeDefinitions, inputPathDefinitions );
        Tap sourceCounts = new Hfs( sourceSchemeCounts, inputPathCounts );

        Scheme sinkScheme = new TextDelimited( new Fields( "dname", "count", "definition" ), " ^^^ " );
        Tap sink = new Hfs( sinkScheme, outputPath, SinkMode.REPLACE );


        Pipe definitionspipe = new Pipe( "definitionspipe" );
        Pipe countpipe = new Pipe( "countpipe" );

        //Join the tuple streams
        Fields commonfields = new Fields( "name" );
        Fields newfields = new Fields("dname", "definition", "cname", "count");
        Pipe joinpipe = new CoGroup( definitionspipe, commonfields, countpipe, commonfields, newfields, new InnerJoin() );

        //Sort
        Fields groupFields = new Fields( "count");
        groupFields.setComparator("count", Collections.reverseOrder());
        Pipe sortedpipe = new GroupBy( joinpipe, groupFields );


        Properties properties = new Properties();
        FlowConnector.setApplicationJarClass(properties, SimplestPipe3CoGroup.class);

        FlowConnector flowConnector = new FlowConnector( properties );
        Map<String, Tap> sources = new HashMap<String, Tap>();
        sources.put("definitionspipe", sourceDefinitions);
        sources.put("countpipe", sourceCounts);
        Flow flow = flowConnector.connect( sources, sink, sortedpipe );
        flow.complete();
    }
}
