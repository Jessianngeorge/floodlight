package net.floodlightcontroller.mactracker;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.projectfloodlight.openflow.protocol.OFFlowAdd;

import org.projectfloodlight.openflow.protocol.OFMessage;
import org.projectfloodlight.openflow.protocol.OFType;
import org.projectfloodlight.openflow.protocol.action.OFAction;
import org.projectfloodlight.openflow.protocol.action.OFActionOutput;
import org.projectfloodlight.openflow.protocol.action.OFActionSetField;
import org.projectfloodlight.openflow.protocol.action.OFActions;
import org.projectfloodlight.openflow.protocol.instruction.OFInstruction;
import org.projectfloodlight.openflow.protocol.instruction.OFInstructionApplyActions;
import org.projectfloodlight.openflow.protocol.instruction.OFInstructions;
import org.projectfloodlight.openflow.protocol.match.Match;
import org.projectfloodlight.openflow.protocol.match.MatchField;
import org.projectfloodlight.openflow.protocol.oxm.OFOxms;
import org.projectfloodlight.openflow.protocol.ver10.OFFactoryVer10;
import org.projectfloodlight.openflow.protocol.ver13.OFFactoryVer13;
import org.projectfloodlight.openflow.types.DatapathId;
import org.projectfloodlight.openflow.types.EthType;
import org.projectfloodlight.openflow.types.IPv4Address;
import org.projectfloodlight.openflow.types.OFBufferId;
import org.projectfloodlight.openflow.types.OFPort;


import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.core.util.SingletonTask;
import net.floodlightcontroller.core.IFloodlightProviderService;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.Set;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.threadpool.IThreadPoolService;
import net.floodlightcontroller.topology.ITopologyService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MACTracker implements IOFMessageListener, IFloodlightModule {
	
	protected IFloodlightProviderService floodlightProvider;
	protected Set<Long> macAddresses;
	protected Set<IOFSwitch> switches = new HashSet<IOFSwitch>();
	protected Map<DatapathId, Set<DatapathId>> islandSwitch = new HashMap<DatapathId, Set<DatapathId>>();
	protected static Logger logger;
	boolean inflow = false;
	private IThreadPoolService threadPoolService;
	private SingletonTask installFlowTask;
	protected ArrayList<OFFlowAdd> flowAdd = new ArrayList<OFFlowAdd>();
	private ITopologyService topologyService;
	private DatapathId minIsland;

	@Override
	public String getName() {
	    return MACTracker.class.getSimpleName();
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
	    Collection<Class<? extends IFloodlightService>> l =
	        new ArrayList<Class<? extends IFloodlightService>>();
	    l.add(IFloodlightProviderService.class);
	    l.add(IThreadPoolService.class);
	    l.add(ITopologyService.class);
	    return l;
	}

	@Override
	public void init(FloodlightModuleContext context) throws FloodlightModuleException {
	    floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
	    threadPoolService = context.getServiceImpl(IThreadPoolService.class);
	    topologyService = context.getServiceImpl(ITopologyService.class);
	    
	     
	    
	    macAddresses = new ConcurrentSkipListSet<Long>();
	    logger = LoggerFactory.getLogger(MACTracker.class);
	    
	    ScheduledExecutorService ses = threadPoolService.getScheduledExecutor();
	 //   To be started by the first switch connection; installs a flow every 30 sec 
	 //   after the first PACKET_IN
	 		installFlowTask = new SingletonTask(ses, new Runnable() {
	 			@Override
	 			public void run() {
	 				try {
	 					findMinIsland();
	 					installFlow();
	 					installFlowTask.reschedule(20, TimeUnit.SECONDS);
	 				} catch (Exception e) {
	 					logger.info("Exception in MacTracker initializing installFlowTask.", e);
	 				}
	 			}
	 		});
	 		
	 		
	}

	@Override
	public void startUp(FloodlightModuleContext context) {
	    floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this);
	    createFlows13();
	}

	@Override
	   public net.floodlightcontroller.core.IListener.Command receive(IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
	        Ethernet eth =
	                IFloodlightProviderService.bcStore.get(cntx,
	                                            IFloodlightProviderService.CONTEXT_PI_PAYLOAD);
	 
	        Long sourceMACHash = eth.getSourceMACAddress().getLong();
	        
	        try{ 
	        	switches.add(sw);
	        	//installFlowTask.reschedule(2, TimeUnit.SECONDS);
	        } catch (Exception e){
	        	logger.info("Something went horribly wrong!!");
	        }
	        
	        if (!macAddresses.contains(sourceMACHash)) {
	            macAddresses.add(sourceMACHash);
	            logger.info("MAC Address: {} seen on switch: {}",
	                    eth.getSourceMACAddress().toString(),
	                    sw.getId().toString());
	            
	        }
	        
	        return Command.CONTINUE;
	    }
	
	
	public void createFlows(){
		
		OFFactoryVer10 myFactory = new OFFactoryVer10();
		Match myMatch = myFactory.buildMatch()
			    .setExact(MatchField.IN_PORT, OFPort.of(1))
			    .setExact(MatchField.ETH_TYPE, EthType.IPv4)
			    .build();
		
		
		logger.info("MyMatch: {}", new Object[] {myMatch.toString()});
		/* Output to a port is also an OFAction, not an OXM. */
		ArrayList<OFAction> actionList = new ArrayList<OFAction>();
		OFActions actions = myFactory.actions();
		OFActionOutput output = actions.buildOutput()
		    .setPort(OFPort.of(2))
		    .build();
		actionList.add(output);
		
		logger.info("My Action: {}", new Object[] {output.toString()});
		logger.info("My Actions: {}", new Object[] {actions.toString()});

		flowAdd.add(myFactory.buildFlowAdd()
				.setBufferId(OFBufferId.NO_BUFFER)
				.setMatch(myMatch)
				.setActions(actionList)
				.setOutPort(OFPort.of(2))
				.build());
		
		myFactory = new OFFactoryVer10();
		myMatch = myFactory.buildMatch()
			    .setExact(MatchField.IN_PORT, OFPort.of(2))
			    .setExact(MatchField.ETH_TYPE, EthType.IPv4)
			    .build();
		
		
		logger.info("MyMatch: {}", new Object[] {myMatch.toString()});
		/* Output to a port is also an OFAction, not an OXM. */
		actionList = new ArrayList<OFAction>();
		actions = myFactory.actions();
		output = actions.buildOutput()
		    .setPort(OFPort.of(1))
		    .build();
		actionList.add(output);
		
		logger.info("My Action: {}", new Object[] {output.toString()});
		logger.info("My Actions: {}", new Object[] {actions.toString()});

		flowAdd.add(myFactory.buildFlowAdd()
				.setBufferId(OFBufferId.NO_BUFFER)
				.setMatch(myMatch)
				.setActions(actionList)
				.setOutPort(OFPort.of(1))
				.build());
		
	}
	
	public void createFlows13(){
		OFFactoryVer13 myFactory = new OFFactoryVer13();
		Match myMatch = myFactory.buildMatch()
			    .setExact(MatchField.IN_PORT, OFPort.of(1))
			    .setExact(MatchField.ETH_TYPE, EthType.IPv4)
			    //.setExact(MatchField.IPV4_SRC,IPv4Address.of("10.0.0.1"))
			    .build();
		
		
		logger.info("MyMatch: {}", new Object[] {myMatch.toString()});
		/* Output to a port is also an OFAction, not an OXM. */
		ArrayList<OFAction> actionList = new ArrayList<OFAction>();
		OFActions actions = myFactory.actions();
		OFOxms oxms = myFactory.oxms();
		OFActionSetField field1 = actions.buildSetField()
		    .setField(
		    		oxms.buildIpv4Dst()
		    		.setValue(IPv4Address.of("10.0.0.2"))
		    		.build()
		    		)
		    .build();
		
		OFActionOutput output = actions.buildOutput()
			    .setMaxLen(0xFFffFFff)
			    .setPort(OFPort.of(2))
			    .build();
		
		
			//actionList.add(field1);
			actionList.add(output);
		
		logger.info("My Action: {}", new Object[] {field1.toString()});
		logger.info("My Actions: {}", new Object[] {actions.toString()});
		/* Supply the OFAction list to the OFInstructionApplyActions. */
		OFInstructions myInstructions = myFactory.instructions();
		OFInstructionApplyActions applyActions = myInstructions.buildApplyActions()
		    .setActions(actionList)
		    .build();
		
		ArrayList<OFInstruction> myInstructionList = new ArrayList<>();
		myInstructionList.add(applyActions);
		
		logger.info("my inst: {}", new Object[] {myInstructions});
		logger.info("apply actions: {}", new Object[] {applyActions});

		flowAdd.add(myFactory.buildFlowAdd()
				.setBufferId(OFBufferId.NO_BUFFER)
				.setHardTimeout(60)
				.setPriority(3000)
				.setMatch(myMatch)
				.setInstructions(myInstructionList)
				.setOutPort(OFPort.of(2))
				.build());
		
		myFactory = new OFFactoryVer13();
		myMatch = myFactory.buildMatch()
			    .setExact(MatchField.IN_PORT, OFPort.of(2))
			    .setExact(MatchField.ETH_TYPE, EthType.IPv4)
			    //.setExact(MatchField.IPV4_SRC,IPv4Address.of("10.0.0.2"))
			    .build();
		
		
		logger.info("MyMatch: {}", new Object[] {myMatch.toString()});
		/* Output to a port is also an OFAction, not an OXM. */
		actionList = new ArrayList<OFAction>();
		actions = myFactory.actions();
		oxms = myFactory.oxms();
		
		output = actions.buildOutput()
			    .setMaxLen(0xFFffFFff)
			    .setPort(OFPort.of(1))
			    .build();
		
		
			//actionList.add(field1);
			actionList.add(output);
		
		
		logger.info("My Action: {}", new Object[] {field1.toString()});
		logger.info("My Actions: {}", new Object[] {actions.toString()});
		myInstructions = myFactory.instructions();
		applyActions = myInstructions.buildApplyActions()
		    .setActions(actionList)
		    .build();
		
		myInstructionList = new ArrayList<>();
		myInstructionList.add(applyActions);

		flowAdd.add(myFactory.buildFlowAdd()
				.setBufferId(OFBufferId.NO_BUFFER)
				.setHardTimeout(60)
				.setPriority(3000)
				.setMatch(myMatch)
				.setInstructions(myInstructionList)
				.setOutPort(OFPort.of(1))
				.build());
		
		myFactory = new OFFactoryVer13();
		myMatch = myFactory.buildMatch()
			    .setExact(MatchField.IN_PORT, OFPort.of(3))
			    .setExact(MatchField.ETH_TYPE, EthType.IPv4)
			    //.setExact(MatchField.IPV4_SRC,IPv4Address.of("10.0.0.3"))
			    .build();
		
		
		logger.info("MyMatch: {}", new Object[] {myMatch.toString()});
		/* Output to a port is also an OFAction, not an OXM. */
		actionList = new ArrayList<OFAction>();
		actions = myFactory.actions();
		oxms = myFactory.oxms();
		
		output = actions.buildOutput()
			    .setMaxLen(0xFFffFFff)
			    .setPort(OFPort.ZERO)
			    .build();
		
		
			//actionList.add(field1);
			actionList.add(output);
		
		
		logger.info("My Action: {}", new Object[] {field1.toString()});
		logger.info("My Actions: {}", new Object[] {actions.toString()});
		myInstructions = myFactory.instructions();
		applyActions = myInstructions.buildApplyActions()
		    .setActions(actionList)
		    .build();
		
		myInstructionList = new ArrayList<>();
		myInstructionList.add(applyActions);

		flowAdd.add(myFactory.buildFlowAdd()
				.setBufferId(OFBufferId.NO_BUFFER)
				.setHardTimeout(60)
				.setPriority(3000)
				.setMatch(myMatch)
				.setInstructions(myInstructionList)
				.setOutPort(OFPort.ZERO)
				.build());
		
		
	}
	
	
	public void findMinIsland(){
		
		logger.info("++++++++++++++++++++++FIND MIN++++++++++++++++++++++++");
		
		Integer min = new Integer(Integer.MAX_VALUE);
		Integer count = new Integer(0);
		
		for(IOFSwitch sw: switches){
			DatapathId id = topologyService.getOpenflowDomainId(sw.getId());
		    islandSwitch.put(id, topologyService.getSwitchesInOpenflowDomain(id));
		    logger.info("Island # : {} {}", new Object[]{id,islandSwitch.get(id)});
		}
		
		if(islandSwitch.isEmpty()){
			logger.info("+++++++++++++++++++++++FIND MIN EXITED++++++++++++++++++++++++");
			return;
		}
		
		if(islandSwitch.keySet().size() < 2){
			logger.info("+++++++++++++++++++++++FIND MIN SMALL KEYSET++++++++++++++++++++++++");
			return;
		}
		
		for (DatapathId DpId: islandSwitch.keySet()){
			count =0;
			
			for (@SuppressWarnings("unused") DatapathId  switchId:  islandSwitch.get(DpId)){
				count++;
			}
			
			logger.info("++++++++++++++++++++++COUNT {} MIN {}++++++++++++++++++++++++", new Object[] {count,min});
			
			if(count<min){
				min = count;
				minIsland = DpId;
				logger.info("++++++++++++++++++++++SET MINISLAND++++++++++++++++++++++++");
			}
		}		
	}
	
	
	public void installFlow(){
		
		logger.info("++++++++++++++++++++++INSTALL FLOWS++++++++++++++++++++++++");
		
		if(switches.isEmpty()){
			return;
		}
		
		if(islandSwitch.isEmpty()){
			logger.info("+++++++++++++++++++++++INSTALL FLOW EXITED++++++++++++++++++++++++");
			return;
		}
		
		if(minIsland != null){
			for(DatapathId DpId: islandSwitch.get(minIsland)){
				
				logger.info("++++++++++++++++++++++GOT MINISLAND++++++++++++++++++++++++");
				
				for(IOFSwitch sw : switches){
					
					if(sw.getId() == DpId){
						if(!sw.isActive()){
							switches.remove(sw);
						}
						
						
						if(sw.isActive()){
							for(OFFlowAdd flow : flowAdd){
								sw.write(flow);
							}
						}
						
						
					}
				}
				
			}
		}

}



}
