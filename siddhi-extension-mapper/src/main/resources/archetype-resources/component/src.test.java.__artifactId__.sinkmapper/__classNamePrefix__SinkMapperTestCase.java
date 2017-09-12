package ${package}.${artifactId}.sinkmapper;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;

public class CSVSinkTestCase {
    private static final Logger log = Logger.getLogger(CSVSinkTestCase.class);
    @BeforeMethod
    public void init() {

    }

    @Test
    public void testCSVSinkmapperDefaultMappingWithSiddhiQL() throws InterruptedException {
        log.info(" ");

        InMemoryBroker.Subscriber changethecorrespondingname = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {

            }
            @Override
            public String getTopic() {
                return " ";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(changethecorrespondingname);

        String streams = "" +
                "@App:name(' ')" +
                "define stream Change_InputStream ( ); " +
                "@sink(type='inMemory', topic=' ', @map(type=' ')) " +
                "define stream Change_OutputStream ( ); ";

        String query = "" +
                "from Change_InputStream " +
                "select * " +
                "insert into Change_OutputStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("Change_InputStream");

        executionPlanRuntime.start();
        stockStream.send(new Object[]{ });

        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals(" ", " ", " ");

        //assert default mapping
        AssertJUnit.assertEquals(" " , " ", " ");

        executionPlanRuntime.shutdown();

        InMemoryBroker.unsubscribe(changethecorrespondingname);
        siddhiManager.shutdown();
    }
}
