package ${package}.streamprocessor;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.testng.AssertJUnit;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

/**
 * Created by kalaiyarasi on 9/5/17.
 */
public class StreamProcessorTestCase {
    private static final Logger log = Logger.getLogger(FunctionTestCase.class);
    int count;

    @Before
    public void init() {

    }

    @Test
    public void linearRegressionTestcase() throws InterruptedException {


        log.info("Simple Regression TestCase");

        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "define stream InputStream (attribute type)";

        String query = ("@info(name = 'query') from InputStream#timeseries:regress(attribute )"
                + "select * "
                + "insert into OutputStream;");
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        executionPlanRuntime.addCallback("query", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                ;
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("InputStream");
        executionPlanRuntime.start();


        Thread.sleep(100);

        AssertJUnit.assertEquals(" ", " ", "");


        executionPlanRuntime.shutdown();
    }
}
