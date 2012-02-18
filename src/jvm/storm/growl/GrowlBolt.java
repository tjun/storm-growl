package storm.growl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.sf.libgrowl.Application;
import net.sf.libgrowl.GrowlConnector;
import net.sf.libgrowl.Notification;
import net.sf.libgrowl.NotificationType;

import org.apache.log4j.Logger;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class GrowlBolt implements IBasicBolt
{
	public static Logger LOG = Logger.getLogger(GrowlBolt.class);
	/*
	 * Growl Conf
	 */
	String name 				="";
	String notificationTypeId 	= "";
	List<String> hosts 			= null;
	int port 					= 0;
	
	/* use siticky notification or not. */
	boolean sticky 				= false;
	/* notification priority */
	int priority 				= 0;
	String iconUrl				= null;
	

	/* 
	 * Constructor
	 */
    public GrowlBolt(GrowlConfig growlConf) {
    	name = growlConf.getName();
    	notificationTypeId = growlConf.getTypeId();
    	
    	if(growlConf.hosts.isEmpty()){
    		LOG.warn("No host registered in GrowlConfig. Use \"localhost\".");
    		hosts = new ArrayList<String>();
    		hosts.add("localhost");
    	}else{
    		hosts = growlConf.hosts;
    	}
    	port = growlConf.port;
    	priority = growlConf.priority;
    	sticky = growlConf.sticky;
    	iconUrl = growlConf.iconUrl;
    }    

    @Override
	public void prepare(Map stormConf, TopologyContext context) {		
	}
    
    /*
     * (non-Javadoc)
     * @see backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple, backtype.storm.topology.BasicOutputCollector)
     */
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        /*
         * tuple must contains Fields named "title", "message" and "iconURL".
         * "title" is used for Growl Title
         * "message" is used for Growl Message
         */
    	
    	String title = tuple.getStringByField("title");
    	String message = tuple.getStringByField("message");
    	
    	
		Application application = new Application(name);
		NotificationType notificationType = new NotificationType(notificationTypeId, name, iconUrl);
		NotificationType[] notificationTypes = new NotificationType[] { notificationType };
		Notification notification = new Notification(application, notificationType, title, message);
		notification.setPriority(priority);
		notification.setSticky(sticky);
		
		for(String host : hosts){
	    	GrowlConnector growl = new GrowlConnector(host, port);
			growl.register(application, notificationTypes);	
			
			growl.notify(notification);
		}
		collector.emit(new Values(title, message));

    }

	@Override
	public void cleanup() {
	}

	/*
	 * (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("title", "message"));
    }

	
}
