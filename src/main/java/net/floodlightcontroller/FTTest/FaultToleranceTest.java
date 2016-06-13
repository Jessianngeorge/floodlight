

package net.floodlightcontroller.FTTest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.IOFSwitchListener;
import net.floodlightcontroller.core.PortChangeType;
import net.floodlightcontroller.core.internal.IOFSwitchService;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.storage.IStorageSourceService;
 
import org.projectfloodlight.openflow.protocol.OFControllerRole;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFPortDesc;
import org.projectfloodlight.openflow.protocol.OFType;
import org.projectfloodlight.openflow.types.DatapathId;
import org.sdnplatform.sync.IStoreClient;
import org.sdnplatform.sync.ISyncService;
import org.sdnplatform.sync.ISyncService.Scope;
import org.sdnplatform.sync.error.SyncException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
public class FaultToleranceTest implements
IOFMessageListener,
IFloodlightModule,
IOFSwitchListener
{
 
	protected IFloodlightProviderService floodlightProvider;
    private ISyncService syncService;
    private IStoreClient<String, String> storeFT;
    protected static Logger logger = LoggerFactory.getLogger(FaultToleranceTest.class);
    protected static IOFSwitchService switchService;
    private String controllerId;
    private Set<String> switches = new HashSet<String>();
	private String activeSwitches= new String("");
 
    @Override
    public String getName() {
        // TODO Auto-generated method stub
        return FaultToleranceTest.class.getSimpleName();
    }
 
    @Override
    public boolean isCallbackOrderingPrereq(OFType type, String name) {
        // TODO Auto-generated method stub
        return false;
    }
 
    @Override
    public boolean isCallbackOrderingPostreq(OFType type, String name) {
        // TODO Auto-generated method stub
        return false;
    }
 
    @Override
    public Collection<Class<? extends IFloodlightService>> getModuleServices() {
        // TODO Auto-generated method stub
        return null;
    }
 
    @Override
    public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
        // TODO Auto-generated method stub
        return null;
    }
 
    @Override
    public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
        // TODO Auto-generated method stub
        Collection<Class<? extends IFloodlightService>> l =
                new ArrayList<Class<? extends IFloodlightService>>();
        l.add(IStorageSourceService.class);
        l.add(ISyncService.class);
        l.add(IOFSwitchService.class);
        return l;
    }
 
    @Override
    public void init(FloodlightModuleContext context)
            throws FloodlightModuleException {
        // TODO Auto-generated method stub
    	floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
         
        this.syncService = context.getServiceImpl(ISyncService.class);
        switchService = context.getServiceImpl(IOFSwitchService.class);
 
        //Map<String, String> configParams = context.getConfigParams(FloodlightProvider.class);
        controllerId = floodlightProvider.getControllerId();
        logger.info("Node Id: {}", new Object[] {controllerId});
    }
 
    @Override
    public void startUp(FloodlightModuleContext context)
            throws FloodlightModuleException {
    	floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
        switchService.addOFSwitchListener(this);
         
        try {
            this.syncService.registerStore("FT_Switches", Scope.GLOBAL);
            this.storeFT = this.syncService
                    .getStoreClient("FT_Switches",
                            String.class,
                            String.class);
            //this.storeFT.addStoreListener(this);
        } catch (SyncException e) {
            throw new FloodlightModuleException("Error while setting up sync service", e);
        }
        
        
    }
 
    @Override
    public net.floodlightcontroller.core.IListener.Command receive(
            IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
        // TODO Auto-generated method stub
    	//String activeSwicthes = getActiveSwitchesAndUpdateSyncInfo();
    	if(!switches.contains(sw.getId().toString())){
    		if(sw.getControllerRole() == OFControllerRole.ROLE_MASTER){
	    		switches.add(sw.getId().toString());
	    		for(String swid: switches){
	    			activeSwitches += swid;
	    		}
	    	
		    	try {
					this.storeFT.put(controllerId, activeSwitches);
				} catch (SyncException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
    		}
    		activeSwitches = "";
    	}
    	logger.info("++++++++++++++++++++++ get active switches ++++++++++++++++++++");
        logger.info("NodeID:{} connected: ", new Object [] {controllerId});
        try {
        	logger.info("Switches in storage for this controller: {}",new Object[] {this.storeFT.get(controllerId).getValue()});
		} catch (SyncException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        logger.info("++++++++++++++++++++++ END ++++++++++++++++++++");
        return Command.CONTINUE;
    }
 
    @Override
    public void switchAdded(DatapathId switchId) {
        // TODO Auto-generated method stub
    }
 
    @Override
    public void switchRemoved(DatapathId switchId) {
        // TODO Auto-generated method stub
        //String activeSwitches = getActiveSwitchesAndUpdateSyncInfo();
        logger.debug("Switch REMOVED: {}, Syncing: {}", switchId, activeSwitches);
    }
 
    @Override
    public void switchActivated(DatapathId switchId) {
        // TODO Auto-generated method stub
        //String activeSwitches = getActiveSwitchesAndUpdateSyncInfo();
        logger.debug("Switch ACTIVATED: {}, Syncing: {}", switchId, activeSwitches);
         
    }
 
    @Override
    public void switchPortChanged(DatapathId switchId, OFPortDesc port,
            PortChangeType type) {
        // TODO Auto-generated method stub
        logger.debug("Switch Port CHANGED: {}", switchId);
    }
 
    @Override
    public void switchChanged(DatapathId switchId) {
        // TODO Auto-generated method stub
        //String activeSwitches = getActiveSwitchesAndUpdateSyncInfo();
        logger.debug("Switch CHANGED: {}, Syncing: {}", switchId, activeSwitches);
         
    }

	@Override
	public void switchDeactivated(DatapathId switchId) {
		// TODO Auto-generated method stub
		
	}
     
}
