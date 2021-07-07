package lazafi.dic2021.exc3;


import at.ac.tuwien.ec.model.infrastructure.MobileCloudInfrastructure;
import at.ac.tuwien.ec.model.infrastructure.computationalnodes.ComputationalNode;
import at.ac.tuwien.ec.model.software.ComponentLink;
import at.ac.tuwien.ec.model.software.MobileApplication;
import at.ac.tuwien.ec.model.software.MobileSoftwareComponent;
import at.ac.tuwien.ec.scheduling.offloading.OffloadScheduler;
import at.ac.tuwien.ec.scheduling.offloading.OffloadScheduling;
import at.ac.tuwien.ec.scheduling.offloading.algorithms.heftbased.utils.NodeRankComparator;
import at.ac.tuwien.ec.scheduling.utils.RuntimeComparator;
import at.ac.tuwien.ec.sleipnir.OffloadingSetup;
import org.jgrapht.graph.DirectedAcyclicGraph;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Set;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

/**
*  Offload Scheduler implementing DLS Algorithm
 * basic steps for scheduling
 *
 * (1) Calculate the bLevel of each computation-task and store in a sorted list
 * Repeat
 * (3) Calculate DL = bLevel - Est on every computation-node
 * (4) Select the task-computation-node pair with the largest DL and schedule it on the computation-node
 */

public class R2Research extends OffloadScheduler {

	public R2Research(MobileApplication A, MobileCloudInfrastructure I) {
		super();
		setMobileApplication(A);
		setInfrastructure(I);
		setRank(this.currentApp,this.currentInfrastructure);
	}

	public R2Research(Tuple2<MobileApplication,MobileCloudInfrastructure> t) {
		super();
		setMobileApplication(t._1());
		setInfrastructure(t._2());
		setRank(this.currentApp,this.currentInfrastructure);
	}

    /**
	 * scheduling is implemented here
     * @return
     */
	@Override
	public ArrayList<? extends OffloadScheduling> findScheduling() {
		double start = System.nanoTime();

		/*scheduledNodes contains the nodes that have been scheduled for execution.
		 * Once nodes are scheduled, they are taken from the PriorityQueue according to their runtime
		 */
		PriorityQueue<MobileSoftwareComponent> scheduledNodes = new PriorityQueue<MobileSoftwareComponent>(new RuntimeComparator());

		/*
		 * tasks contains tasks that have to be scheduled for execution.
		 * this list is used to keep track of the allready scheduled tastks
		*/
		PriorityQueue<MobileSoftwareComponent> tasks = new PriorityQueue<MobileSoftwareComponent>(new NodeRankComparator());

		// the ready note pool
		// initially we fill it witch root nodes of the Application
		List<MobileSoftwareComponent> taskPool = currentApp.getTaskDependencies().vertexSet().stream()
				.filter(vert -> currentApp.getTaskDependencies().incomingEdgesOf(vert).size() == 0)
				.collect(Collectors.toList());


		tasks.addAll(currentApp.getTaskDependencies().vertexSet());
		ArrayList<OffloadScheduling> deployments = new ArrayList<OffloadScheduling>();
				
		MobileSoftwareComponent currTask;
		//We initialize a new OffloadScheduling object, modelling the scheduling computer with this algorithm
		OffloadScheduling scheduling = new OffloadScheduling();

		double dlMax = 0.0;
//		ComputationalNode target = null;

		// we run till all tasks are scheduled
		while(tasks.size() > 0) {
            System.out.println(tasks.size());
			PriorityQueue<Tuple3<MobileSoftwareComponent, ComputationalNode, Double>> candidates
					= new PriorityQueue<Tuple3<MobileSoftwareComponent, ComputationalNode, Double>>(new CandidatesComparator());


			for (MobileSoftwareComponent t: taskPool) {
				if (!t.isOffloadable()
						&& isValid(scheduling, t, (ComputationalNode) currentInfrastructure.getNodeById(t.getUserId()))) {
					// If task is not offloadable, deploy it in the mobile device (if enough resources are available)
					ComputationalNode target1 = (ComputationalNode) currentInfrastructure.getNodeById(t.getUserId());
					deploy(scheduling, t, target1);
					scheduledNodes.add(t);
					tasks.remove(t);
				} else {
					//Check for all available Cloud/Edge nodes
					for (ComputationalNode cn : currentInfrastructure.getAllNodes()) {
						double dl = t.getRank() - cn.getESTforTask(t);
						Tuple3<MobileSoftwareComponent, ComputationalNode, Double> candidate = new Tuple3<>(t, cn, dl);
						candidates.add(candidate);
					}

					Tuple3<MobileSoftwareComponent, ComputationalNode, Double> winning =  candidates.poll();

					//if scheduling found a target node for the task, it allocates it to the target node
					if(winning != null)
					{
						deploy(scheduling,winning._1(),winning._2());
						scheduledNodes.add(winning._1());
						tasks.remove(winning._1());
					}
					else if(!scheduledNodes.isEmpty())
					{
						MobileSoftwareComponent terminated = scheduledNodes.remove();
						((ComputationalNode) scheduling.get(terminated)).undeploy(terminated);
					}
					/*
					 * if simulation considers mobility, perform post-scheduling operations
					 * (default is to update coordinates of mobile devices)
					 */
					if(OffloadingSetup.mobility) {
						postTaskScheduling(scheduling);
					}
				}
			}

			// next nodes
			//taskPool = currentApp.getTaskDependencies().vertexSet().stream()
			//		.flatMap(vert -> currentApp.getTaskDependencies().outgoingEdgesOf(vert))
			//		.collect(Collectors.toList());

			ArrayList<MobileSoftwareComponent> newTaskPool = new ArrayList<>();
			for (MobileSoftwareComponent c : taskPool) {
				java.util.Set<ComponentLink> out =  currentApp.getTaskDependencies().outgoingEdgesOf(c);
				for (ComponentLink cl : out) {
					newTaskPool.add(cl.getTarget());
				}
			}
			System.out.println("n" + newTaskPool.size());
			if (newTaskPool.isEmpty()) {
				taskPool.addAll(tasks);
			} else {
				taskPool = newTaskPool;
			}

		}
		double end = System.nanoTime();
		scheduling.setExecutionTime(end-start);
		deployments.add(scheduling);
		return deployments;
	}

	protected void setRank(MobileApplication A, MobileCloudInfrastructure I)
	{
		for(MobileSoftwareComponent msc : A.getTaskDependencies().vertexSet())
			msc.setVisited(false);
				
		for(MobileSoftwareComponent msc : A.getTaskDependencies().vertexSet())		
			upRank(msc,A.getTaskDependencies(),I);

	}

    /**
     * calculate static b-levels used to rank the tasks
     * rank is computed recuversively by traversing the task graph upward
     * @param msc
     * @param dag Mobile Application's DAG
     * @param infrastructure
     * @return the upward rank of msc
     * (which is also the lenght of the critical path (CP) of this task to the exit task)
     */
	private double upRank(MobileSoftwareComponent msc, DirectedAcyclicGraph<MobileSoftwareComponent, ComponentLink> dag,
			MobileCloudInfrastructure infrastructure) {
		double w_cmp = 0.0; // average execution time of task on each processor / node of this component
		if(!msc.isVisited())
        /*  since upward Rank is defined recursively, visited makes sure no extra unnecessary computations are done when
		    calling upRank on all nodes during initialization */
        {
			msc.setVisited(true);
			int numberOfNodes = infrastructure.getAllNodes().size() + 1;
			for(ComputationalNode cn : infrastructure.getAllNodes())
				w_cmp += msc.getLocalRuntimeOnNode(cn, infrastructure);
			
			w_cmp = w_cmp / numberOfNodes;

            double tmpWRank;
            double maxSRank = 0; // max successor rank
            for(ComponentLink neigh : dag.outgoingEdgesOf(msc)) // for the exit task rank=w_cmp
            {
                // rank = w_Cmp +  max(cij + rank(j)    for all j in succ(i)
                // where cij is the average commmunication cost of edge (i, j)
                tmpWRank = upRank(neigh.getTarget(),dag,infrastructure); // succesor's rank
                double tmpCRank = 0;  // this component's average Communication rank
                //We consider only offloadable successors. If a successor is not offloadable, communication cost is 0
                if(neigh.getTarget().isOffloadable()) 
                {
                    for(ComputationalNode cn : infrastructure.getAllNodes())
    					// we do not add transitionTime for static b-level
                        //tmpCRank += infrastructure.getTransmissionTime(neigh.getTarget(), infrastructure.getNodeById(msc.getUserId()), cn);
                    tmpCRank = tmpCRank / (infrastructure.getAllNodes().size());
                }
                double tmpRank = tmpWRank + tmpCRank;
                maxSRank = (tmpRank > maxSRank)? tmpRank : maxSRank;
            }
            msc.setRank(w_cmp + maxSRank);
		}
		return msc.getRank();
	}
	
}
