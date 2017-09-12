package ${package}.function;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
/*import org.wso2.siddhi.core.stream.output.sink.InMemorySink;*/
import org.wso2.siddhi.core.util.EventPrinter;

public class ${siddhi_function_name}TestCaseofFunction {
    private static final Logger log = Logger.getLogger(${siddhi_function_name}TestCaseofFunction.class);
    @Test
    public void testWritefile() throws InterruptedException {
        log.info("__________Function TestCase_______________");
        SiddhiManager siddhiManager = new SiddhiManager();
        // siddhiManager.setExtension("custom:plus", CustomFunctionExtension.class);

        String streams = "" +
                "define stream cseEventStream (symbol string, volume1 long, volume2 long);";

        String query = "" +
                "@info(name = 'query') " +
                "from cseEventStream " +
                "select symbol , custom:total(volume1,volume2) as totalVolume " +
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


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");


        executionPlanRuntime.start();

        //Sending events to Siddhi
        inputHandler.send(new Object[]{"IBM", 700L, 100L});
        inputHandler.send(new Object[]{"WSO2", 600L, 200L});
        inputHandler.send(new Object[]{"GOOG", 60L, 200L});
        Thread.sleep(500);


        Thread.sleep(100);
        //AssertJUnit.assertEquals(1, wso2Count);
        executionPlanRuntime.shutdown();
        siddhiManager.shutdown();
    }


}
