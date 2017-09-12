package ${package}.window;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.sink.InMemorySink;
import org.wso2.siddhi.core.util.EventPrinter;


public class WindowTestCase {
    private static final Logger log = Logger.getLogger(CountTestCase.class);
    @Before
    public void init() {

    }
    @Test
    public void testLengthWindowProcessor() throws InterruptedException {
        log.info("   ");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define streaM inputStream (ATTRIBUTE TYPE);";

        String query = "" +
                "@info(name = 'query') " +
                "from inputStream#window.custom:customWindow(4) " +
                "select ATTRIBUTE,ATTRIBUTE,ATTRIBUTE " +
                "insert all events into outputStream ;";

        siddhiManager.setExtension("sink:inMemory", InMemorySink.class);
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        executionPlanRuntime.addCallback("query", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                Assert.assertEquals(" ", " ", "");

            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[]{  });
        Thread.sleep(500);
        executionPlanRuntime.shutdown();
    }
}
