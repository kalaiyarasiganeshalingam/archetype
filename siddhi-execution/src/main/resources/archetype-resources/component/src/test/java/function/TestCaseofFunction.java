package ${package}.function;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

public class FunctionTestCase {
    private static final Logger log = Logger.getLogger(FunctionTestCase.class);
    @Test
    public void testWritefile() throws InterruptedException {
        log.info("   ");
        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "" +
                "define stream InputStream (attribute type, attribute type);";

        String query = "" +
                "@info(name = 'query') " +
                "from OutputStream " +
                "select symbol , custom:total(attribute,attribute) as totalVolume " +
                "insert into Output;";

        //siddhiManager.setExtension("sink:inMemory", InMemorySink.class);
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        //InputHandler stockStream = executionPlanRuntime.getInputHandler("Output");
        //Retrieving InputHandler to push events into Siddhi
        executionPlanRuntime.addCallback("query", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);

            }
        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("InputStream");
        executionPlanRuntime.start();
        //Sending events to Siddhi
        inputHandler.send(new Object[]{ });
        Thread.sleep(500);
        Assert.assertEquals(" ", " ", "");
        executionPlanRuntime.shutdown();
        siddhiManager.shutdown();
    }

}
