package ${package}.streamfunction;

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


public class ${siddhi_function_name}TestcaseofStreamFunctionProcessor {
    private static final Logger log = Logger.getLogger(${siddhi_function_name}TestcaseofStreamFunctionProcessor.class);
    private static int eventCount = 0;

    @Test
    public void testProcess() throws Exception {
        log.info("_______Init Siddhi setUp___________");
        SiddhiManager siddhiManager = new SiddhiManager();
        //siddhiManager.setExtension("custom:streamfunction", StreamFunctionProcessorExtension.class);
        String streams = "" +
                "define stream geocodeStream (location string, level string, time string);";

        String query = "" +
                "@info(name = 'query') " +
                "from geocodeStream#custom:geocoder(location)" +
                "select latitude, longitude, formattedAddress " +
                "insert into dataOut;";
        //siddhiManager.setExtension("sink:inMemory", InMemorySink.class);
        siddhiManager.setExtension("custom:streamfunction", StreamFunctionProcessorExtension.class);
        long start = System.currentTimeMillis();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        long end = System.currentTimeMillis();
        log.info(String.format("Time to add query: [%f sec]", ((end - start) / 1000f)));
        List<Object[]> data = new ArrayList<>();

        data.add(new Object[]{"gunasekara mawatha,Matara", "Regular", "Mon Aug 02 13:36:05 +0000 2017"});
        data.add(new Object[]{"hendala road", "Regular", "Mon Aug 12 13:36:05 +0000 2017"});
        data.add(new Object[]{"mt lavinia", "Regular", "Mon Aug 10 13:36:05 +0000 2017"});
        //data.add(new Object[]{"duplication rd", "Regular", "Mon Aug 02 13:36:05 +0000 2017"});
        final List<Object[]> expectedResult = new ArrayList<Object[]>();
        expectedResult.add(new Object[]{5.9461591d, 80.498026d, "Gunasekara Mawatha, Matara, Sri Lanka"});
        expectedResult.add(new Object[]{6.995512d, 79.882999d, "Hendala Rd, Wattala, Sri Lanka"});
        expectedResult.add(new Object[]{6.830118d, 79.880083d, "Dehiwala-Mount Lavinia, Sri Lanka"});
        //expectedResult.add(new Object[]{6.912746d, 79.851362d , "Sri Uttarananda Mawatha, Colombo, Sri Lanka"});
        executionPlanRuntime.addCallback("query", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    //log.info(event.getData(0));
                    Object[] expected = expectedResult.get(eventCount);
                    //log.info(eventCount);
                    Assert.assertEquals((Double) expected[0], (Double) event.getData(0), 1e-6);
                    Assert.assertEquals((Double) expected[1], (Double) event.getData(1), 1e-6);
                    Assert.assertEquals(expected[2], event.getData(2));
                    eventCount++;
                }
            }
        });
        executionPlanRuntime.start();
        Thread.sleep(100);
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("geocodeStream");
        for (Object[] dataLine : data) {
            inputHandler.send(dataLine);
            Thread.sleep(200);
        }
    }

}
