package at.ac.tuwien.ec.scheduling.offloading.algorithms.etf;

import at.ac.tuwien.ec.model.infrastructure.MobileCloudInfrastructure;
import at.ac.tuwien.ec.model.infrastructure.computationalnodes.ComputationalNode;
import at.ac.tuwien.ec.model.software.ComponentLink;
import at.ac.tuwien.ec.model.software.MobileApplication;
import at.ac.tuwien.ec.model.software.MobileSoftwareComponent;
import at.ac.tuwien.ec.scheduling.Scheduling;
import at.ac.tuwien.ec.scheduling.offloading.OffloadScheduler;
import at.ac.tuwien.ec.scheduling.offloading.OffloadScheduling;
import at.ac.tuwien.ec.scheduling.offloading.algorithms.etf.utils.NodeRankComparator;
import at.ac.tuwien.ec.scheduling.utils.RuntimeComparator;
import at.ac.tuwien.ec.sleipnir.OffloadingSetup;
import org.jgrapht.graph.DirectedAcyclicGraph;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.PriorityQueue;

// IMPLEMENTED BY ESZTER HORVÁTH 12042300

public class ETFResearch extends OffloadScheduler {

    /**
     * @param A MobileApplication property from SimIteration
     * @param I MobileCloudInfrastructure property from SimIteration
     * Constructors set the parameters and calls setRank() to nodes' ranks
     */

    public ETFResearch(MobileApplication A, MobileCloudInfrastructure I) {
        super();
        setMobileApplication(A);
        setInfrastructure(I);
        setRank(A, I);
    }

    public ETFResearch(Tuple2<MobileApplication,MobileCloudInfrastructure> t) {
        super();
        setMobileApplication(t._1());
        setInfrastructure(t._2());
        setRank(t._1(), t._2());
    }

    @Override
    public ArrayList<? extends Scheduling> findScheduling() {
        double start = System.nanoTime();

        ArrayList<OffloadScheduling> schedulings = new ArrayList<>();
        // taskList contains tasks that have to be scheduled for execution
        PriorityQueue<MobileSoftwareComponent> taskList = new PriorityQueue<>(new NodeRankComparator());
        taskList.addAll(currentApp.getTaskDependencies().vertexSet());
        // scheduledNodes contains the already scheduled nodes
        PriorityQueue<MobileSoftwareComponent> scheduledNodes = new PriorityQueue<>(new RuntimeComparator());

        OffloadScheduling scheduling = new OffloadScheduling();

        MobileSoftwareComponent currTask;
        // iterating through all tasks
        while((currTask = taskList.peek()) != null)
        {
            ComputationalNode target = null;
            double EST = Double.MAX_VALUE;

            ComputationalNode localDevice = (ComputationalNode) currentInfrastructure.getNodeById(currTask.getUserId());
            // if the task if not offloadable, it must be computed on the local device
            if(!currTask.isOffloadable()) {
                if (isValid(scheduling, currTask, localDevice))
                    target = localDevice;
            }
            // but if it is offloadable, i check the edge and cloud nodes
            else
            {
                // iterating through all the edge and cloud nodes to find the one with smallest EST
                // EST: earliest start time
                for(ComputationalNode cn : currentInfrastructure.getAllNodes()) {
                    double currEst = cn.getESTforTask(currTask);
                    // i am looking for the smallest EST but i only consider the valid nodes
                    if (currEst < EST && isValid(scheduling, currTask, cn)) {
                        EST = cn.getESTforTask(currTask);
                        target = cn;
                    }
                }

                // i check the local EST as well because it is possible that the task is offloadable
                // but it is better to compute it locally though
                if(localDevice.getESTforTask(currTask) < EST && isValid(scheduling,currTask,localDevice)) {
                    target = localDevice;
                }
            }

            // if scheduling found a target node for the task, it allocates it to the target node
            if(target != null) {
                deploy(scheduling, currTask, target);
                scheduledNodes.add(currTask);
                taskList.remove(currTask);
            }
            else if(!scheduledNodes.isEmpty()) {
                MobileSoftwareComponent terminated = scheduledNodes.remove();
                ((ComputationalNode) scheduling.get(terminated)).undeploy(terminated);
            }
            //if simulation considers mobility, perform post-scheduling operations
            if(OffloadingSetup.mobility)
                postTaskScheduling(scheduling);
        }
        double end = System.nanoTime();
        scheduling.setExecutionTime(end-start);
        schedulings.add(scheduling);
        return schedulings;
    }

    protected void setRank(MobileApplication A, MobileCloudInfrastructure I)
    {
        for(MobileSoftwareComponent msc : A.getTaskDependencies().vertexSet())
            msc.setVisited(false);

        for(MobileSoftwareComponent msc : A.getTaskDependencies().vertexSet())
            calculateStaticBLevels(msc, A.getTaskDependencies(), I);

    }

    // ties are broken by static b-level in ETF
    private double calculateStaticBLevels(MobileSoftwareComponent msc,
                                        DirectedAcyclicGraph<MobileSoftwareComponent, ComponentLink> dag,
                                        MobileCloudInfrastructure infrastructure) {
        if(!msc.isVisited()) {
            msc.setVisited(true);

            // static b-level does not consider the edge weight so i do not add the transmission time here
            // only the node weights which can be calculated through the local runtimes
            double rank = msc.getLocalRuntimeOnNode(
                    (ComputationalNode) infrastructure.getNodeById(msc.getUserId()), infrastructure);
            double maxNeighborRank = 0.0;

            // find the neighbor with the highest rank
            for (ComponentLink neighbor : dag.outgoingEdgesOf(msc)) {
                double neighborRank = calculateStaticBLevels(neighbor.getTarget(), dag, infrastructure);
                maxNeighborRank = Math.max(neighborRank, maxNeighborRank);
            }

            // the rank of the actual node is the sum of its highest neighbor rank and its own local runtime
            msc.setRank(maxNeighborRank + rank);
        }
        return msc.getRank();
    }
}
