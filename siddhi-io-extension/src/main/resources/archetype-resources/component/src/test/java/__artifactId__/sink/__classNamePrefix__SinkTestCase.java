package ${package}.${artifactId}.sink;

import org.apache.log4j.Logger;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;

public class ${artifactId}SinkTestCase {
    private static final Logger log = Logger.
        getLogger(${siddhi_function_name}SinkTestCase.class);
    @Test
    public void testWritefile() throws InterruptedException {
        log.info("______________Test to write the contents into the file ______________");
        // Creating Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@App:name('TestExecutionPlan')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='file',file.name='output.xml',folder.path='Files',@map(type='xml'))" +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        //Generating runtime
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        //Retrieving InputHandler to push events into Siddhi
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");
        //Starting event processing
        executionPlanRuntime.start();
        //Sending events to Siddhi
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        Thread.sleep(100);
        //Shutting down the runtime
        executionPlanRuntime.shutdown();
        //Shutting down Siddhi
        siddhiManager.shutdown();
    }
    @Test
    public void testJsonOutput() throws InterruptedException {
        log.info("______________Test to write the contents into the file ______________");
        // Creating Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@App:name('TestExecutionPlan')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='file',file.name='output.json',@map(type='json'))" +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        //Generating runtime
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        //Retrieving InputHandler to push events into Siddhi
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");
        //Starting event processing
        executionPlanRuntime.start();
        //Sending events to Siddhi
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        Thread.sleep(100);
        //Shutting down the runtime
        executionPlanRuntime.shutdown();
        //Shutting down Siddhi
        siddhiManager.shutdown();
    }

    @Test
    public void testWriteFilewithEvents() throws InterruptedException {
        log.info("______________Test to write the contents into the file ______________");
        // Creating Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@App:name('TestExecutionPlan')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='file',file.name='output.xml',folder.path='Files',@map(type='xml'))" +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        //Generating runtime
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        //Retrieving InputHandler to push events into Siddhi
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");
        //Starting event processing
        executionPlanRuntime.start();
        //Sending events to Siddhi
        Event wso2Event = new Event(System.currentTimeMillis() , new Object[]{"WSO2#@$" , 55.6f , 100L});
        Event ibmEvent = new Event(System.currentTimeMillis() , new Object[]{"IBM" , 75.6f , 100L});
        stockStream.send(new Event[]{wso2Event , ibmEvent});
        Thread.sleep(100);
        //Shutting down the runtime
        executionPlanRuntime.shutdown();
        //Shutting down Siddhi
        siddhiManager.shutdown();
    }
    @Test
    public void testJsonOutputwithEvents() throws InterruptedException {
        log.info("______________Test to write the contents into the file ______________");
        // Creating Siddhi Manager
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "@App:name('TestExecutionPlan')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='file',file.name='output.json',@map(type='json'))" +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        //Generating runtime
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        //Retrieving InputHandler to push events into Siddhi
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");
        //Starting event processing
        executionPlanRuntime.start();
        //Sending events to Siddhi
        Event wso2Event = new Event(System.currentTimeMillis() , new Object[]{"WSO2#@$" , 55.6f , 100L});
        Event ibmEvent = new Event(System.currentTimeMillis() , new Object[]{"IBM" , 75.6f , 100L});
        stockStream.send(new Event[]{wso2Event , ibmEvent});
        Thread.sleep(100);
        //Shutting down the runtime
        executionPlanRuntime.shutdown();
        //Shutting down Siddhi
        siddhiManager.shutdown();
    }



}
