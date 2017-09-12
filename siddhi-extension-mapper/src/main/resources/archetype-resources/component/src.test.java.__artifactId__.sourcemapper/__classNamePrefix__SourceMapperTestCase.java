package ${package}.${artifactId}.sourcemapper;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.concurrent.atomic.AtomicInteger;


public class CSVSourceTestCase {
    private static final Logger log = Logger.getLogger(CSVSourceTestCase.class);
    private AtomicInteger count = new AtomicInteger();


    @Test
    public void testXmlInputDefaultMapping() throws InterruptedException {
        log.info("   ");
        String streams = "" +
                "@App:name(' ')" +
                "@source(type=' ', @map(type=' ')) " +
                "define stream InputStream ( ); " +
                "define stream OutputStream ( ); ";

        String query = "" +
                "from InputStream " +
                "select * " +
                "insert into OutputStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("InputStream",  new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    int n = count.getAndIncrement();
                    switch (n) {
                        case 0:
                            AssertJUnit.assertEquals(" ", "");
                            break;
                        case 1:
                            AssertJUnit.assertEquals(" ", " ");
                            break;

                        default:
                            AssertJUnit.fail(" ");
                    }
                }
            }
        });

        siddhiAppRuntime.start();
        //assert event count
        AssertJUnit.assertEquals(" ", " ", "");
        siddhiAppRuntime.shutdown();
    }
}
