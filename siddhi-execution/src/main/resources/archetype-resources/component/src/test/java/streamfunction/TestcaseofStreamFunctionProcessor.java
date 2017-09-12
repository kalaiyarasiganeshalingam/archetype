import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.ArrayList;
import java.util.List;

public class StreamFunctionTestCase {
    private static final Logger log = Logger.getLogger(FunctionTestCase.class);

    @Test
    public void testProcess() throws Exception {
        log.info(" ");
        SiddhiManager siddhiManager = new SiddhiManager();
        //siddhiManager.setExtension("custom:streamfunction", StreamFunctionProcessorExtension.class);
        String streams = "" +
                "define stream InputStream (attribute type, attribute type);";

        String query = "" +
                "@info(name = 'query') " +
                "from InputStream#custom:geocoder(attribute)" +
                "select attribute, attribute, attribute " +
                "insert into OutputStream;";
        //siddhiManager.setExtension("sink:inMemory", InMemorySink.class);
        siddhiManager.setExtension("custom:streamfunction", StreamFunction.class);
        long start = System.currentTimeMillis();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        long end = System.currentTimeMillis();
        log.info(String.format("Time to add query: [%f sec]", ((end - start) / 1000f)));
        List<Object[]> data = new ArrayList<>();

        data.add(new Object[]{ });

        final List<Object[]> expectedResult = new ArrayList<Object[]>();
        expectedResult.add(new Object[]{ });

        executionPlanRuntime.addCallback("query", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {

                    //Object[] expected = expectedResult.get(eventCount);
                    //log.info(eventCount);
                    Assert.assertEquals(" ", " ", "");

                }
            }
        });
        executionPlanRuntime.start();
        Thread.sleep(100);
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("InputStream");
        for (Object[] dataLine : data) {
            inputHandler.send(dataLine);
            Thread.sleep(200);
        }
    }
}
