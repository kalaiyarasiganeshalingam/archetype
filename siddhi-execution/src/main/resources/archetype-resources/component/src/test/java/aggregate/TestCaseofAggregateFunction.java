package ${package}.aggregate

import org.apache.log4j.Logger;
import org.junit.Before;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;


public class CountTestCase {
    private static final Logger log = Logger.getLogger(CountTestCase.class);
    private Object count;

    @Before
    public void init() {

        count = 0;
    }
    @Test
    public void testAggregateFunction() throws InterruptedException {
        log.info("   ");
        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "define stream InputStream (attribute type ); ";
        String query = "" + "@info(name = 'query') " + "from InputStream#window.time(1 sec)" +
                "select custom:aggregator(attribute) as totalOrders" +
                " insert into OutputStream;";

        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        executionPlanRuntime.addCallback("query", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count = event.getData(0);
                }
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("InputStream");
        executionPlanRuntime.start();
        Thread.sleep(100);
        inputHandler.send(new Object[]{});
        Thread.sleep(100);
        executionPlanRuntime.shutdown();
        AssertJUnit.assertEquals(" ", " ", "");
    }
}
