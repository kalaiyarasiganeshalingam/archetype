package ${package}.window;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.sink.InMemorySink;
import org.wso2.siddhi.core.util.EventPrinter;


public class ${siddhi_function_name}TestCaseofWindow {

    private static Logger logger = Logger.getLogger(TestCaseofWindow.class);
    protected static SiddhiManager siddhiManager;
    private int inEventCount;
    private int removeEventCount;
    private int count;
    private boolean eventArrived;

    @Before
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }
    @Test
    public void testLengthWindowProcessor() throws InterruptedException {
        logger.info("Testing TestCaseofWindow length window with no of events less than window size");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";

        String query = "" +
                "@info(name = 'query') " +
                "from cseEventStream#window.custom:customWindow(4) " +
                "select symbol,price,volume " +
                "insert all events into outputStream ;";

        siddhiManager.setExtension("sink:inMemory", InMemorySink.class);
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        executionPlanRuntime.addCallback("query", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                Assert.assertEquals("Message order inEventCount", inEventCount, inEvents[0].getData(2));
                Assert.assertEquals("Events cannot be expired", false, inEvents[0].isExpired());
                inEventCount = inEventCount + inEvents.length;
                eventArrived = true;
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 1});
        inputHandler.send(new Object[]{"IBM1", 700f, 2});

        Thread.sleep(500);
        Assert.assertEquals(3, inEventCount);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testLengthWindow() throws InterruptedException {
        logger.info("Testing TestCaseofWindow length window with no of events greater than window size");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream cseEventStream (symbol string, price float, volume int);";

        String query = "" +
                "@info(name = 'query') " +
                "from cseEventStream#window.custom:customWindow(4) " +
                "select symbol,price,volume " +
                "insert all events into outputStream ;";

        siddhiManager.setExtension("custom:window", org.wso2.siddhi.window.WindowFunction.class);
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        executionPlanRuntime.addCallback("query", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (removeEvents == null) {
                    Assert.assertEquals("Events cannot be expired", false, inEvents[0].isExpired());
                    inEventCount = inEventCount + inEvents.length;
                } else  {
                    removeEventCount = removeEventCount + inEvents.length;
                    Assert.assertEquals("Events  expired", false, inEvents[0].isExpired());
                }
                eventArrived = true;
            }

        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{"IBM", 700f, 0});
        inputHandler.send(new Object[]{"WSO2", 60.5f, 1});
        inputHandler.send(new Object[]{"IBM1", 700f, 2});
        inputHandler.send(new Object[]{"WSO21", 60.5f, 3});
        inputHandler.send(new Object[]{"IBM2", 700f, 4});
        inputHandler.send(new Object[]{"WSO22", 60.5f, 5});
        Thread.sleep(500);
        Assert.assertEquals(4, inEventCount);
        Assert.assertTrue(eventArrived);
        Assert.assertEquals(2, removeEventCount);

        executionPlanRuntime.shutdown();
    }

}
