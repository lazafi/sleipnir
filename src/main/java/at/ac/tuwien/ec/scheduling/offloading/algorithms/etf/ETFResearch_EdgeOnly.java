package at.ac.tuwien.ec.scheduling.offloading.algorithms.etf;

import at.ac.tuwien.ec.model.infrastructure.MobileCloudInfrastructure;
import at.ac.tuwien.ec.model.infrastructure.computationalnodes.ComputationalNode;
import at.ac.tuwien.ec.model.infrastructure.computationalnodes.EdgeNode;
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

public class ETFResearch_EdgeOnly extends OffloadScheduler {

    /**
     * @param A MobileApplication property from SimIteration
     * @param I MobileCloudInfrastructure property from SimIteration
     * Constructors set the parameters and calls setRank() to nodes' ranks
     */

    public ETFResearch_EdgeOnly(MobileApplication A, MobileCloudInfrastructure I) {
        super();
        setMobileApplication(A);
        setInfrastructure(I);
        setRank(A, I);
    }

    public ETFResearch_EdgeOnly(Tuple2<MobileApplication,MobileCloudInfrastructure> t) {
        super();
        setMobileApplication(t._1());
        setInfrastructure(t._2());
        setRank(t._1(), t._2());
    }

    @Override
    public ArrayList<? extends Scheduling> findScheduling() {
        double start = System.nanoTime();

        ArrayList<OffloadScheduling> schedulings = new ArrayList<>();
        PriorityQueue<MobileSoftwareComponent> scheduledNodes = new PriorityQueue<>(new RuntimeComparator());
        PriorityQueue<MobileSoftwareComponent> taskList = new PriorityQueue<>(new NodeRankComparator());
        taskList.addAll(currentApp.getTaskDependencies().vertexSet());

        OffloadScheduling scheduling = new OffloadScheduling();

        MobileSoftwareComponent currTask;
        while((currTask = taskList.peek()) != null)
        {
            ComputationalNode target = null;
            double EST = Double.MAX_VALUE;

            ComputationalNode localDevice = (ComputationalNode) currentInfrastructure.getNodeById(currTask.getUserId());
            // even though the goal is to use EDGE only, the not offloadable tasks must be computed locally
            if(!currTask.isOffloadable()) {
                if (isValid(scheduling, currTask, localDevice))
                    target = localDevice;
            }
            else
            {
                // but if the tasks is offloadable, i check the EDGE nodes
                // so instead of getAllNodes, i used getEdgeNodes
                for(EdgeNode en : currentInfrastructure.getEdgeNodes().values()) {
                    double currEst = en.getESTforTask(currTask);
                    if (currEst < EST && isValid(scheduling, currTask, en)) {
                        EST = en.getESTforTask(currTask);
                        target = en;
                    }
                }
            }

            if(target != null) {
                deploy(scheduling, currTask, target);
                scheduledNodes.add(currTask);
                taskList.remove(currTask);
            }
            else if(!scheduledNodes.isEmpty()) {
                MobileSoftwareComponent terminated = scheduledNodes.remove();
                ((ComputationalNode) scheduling.get(terminated)).undeploy(terminated);
            }

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

    private double calculateStaticBLevels(MobileSoftwareComponent msc,
                                          DirectedAcyclicGraph<MobileSoftwareComponent, ComponentLink> dag,
                                          MobileCloudInfrastructure infrastructure) {
        if(!msc.isVisited()) {
            msc.setVisited(true);

            double rank = msc.getLocalRuntimeOnNode(
                    (ComputationalNode) infrastructure.getNodeById(msc.getUserId()), infrastructure);
            double maxNeighborRank = 0.0;

            for (ComponentLink neighbor : dag.outgoingEdgesOf(msc)) {
                double neighborRank = calculateStaticBLevels(neighbor.getTarget(), dag, infrastructure);
                maxNeighborRank = Math.max(neighborRank, maxNeighborRank);
            }

            msc.setRank(maxNeighborRank + rank);
        }
        return msc.getRank();
    }
}
