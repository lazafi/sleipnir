package lazafi.dic2021.exc3;

import at.ac.tuwien.ec.model.infrastructure.MobileCloudInfrastructure;
import at.ac.tuwien.ec.model.infrastructure.computationalnodes.ComputationalNode;
import at.ac.tuwien.ec.model.software.ComponentLink;
import at.ac.tuwien.ec.model.software.MobileApplication;
import at.ac.tuwien.ec.model.software.MobileSoftwareComponent;
import at.ac.tuwien.ec.scheduling.Scheduling;
import at.ac.tuwien.ec.scheduling.offloading.OffloadScheduler;
import at.ac.tuwien.ec.scheduling.offloading.OffloadScheduling;
import at.ac.tuwien.ec.scheduling.utils.RuntimeComparator;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.jgrapht.graph.DirectedAcyclicGraph;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.PriorityQueue;

public class RandomOffloadScheduler extends OffloadScheduler {

    public RandomOffloadScheduler(MobileApplication A, MobileCloudInfrastructure I) {
        super();
        setMobileApplication(A);
        setInfrastructure(I);
    }

    public RandomOffloadScheduler(Tuple2<MobileApplication, MobileCloudInfrastructure> t) {
        super();
        setMobileApplication(t._1());
        setInfrastructure(t._2());
    }

    @Override
    public ArrayList<? extends Scheduling> findScheduling() {
        ArrayList<OffloadScheduling> schedulings = new
                ArrayList<>();

        PriorityQueue<MobileSoftwareComponent> taskList
                = new PriorityQueue<MobileSoftwareComponent>(new RuntimeComparator());

        DirectedAcyclicGraph<MobileSoftwareComponent, ComponentLink> deps =
                this.getMobileApplication().getTaskDependencies();
        Iterator<MobileSoftwareComponent> it = deps.iterator();
        while(it.hasNext()) {
            taskList.add(it.next());
        }

        OffloadScheduling scheduling = new OffloadScheduling();
        ComputationalNode target = null;
        for(MobileSoftwareComponent currTask : taskList) {
            if (!currTask.isOffloadable()) {
                if (isValid(scheduling, currTask, (ComputationalNode)
                        currentInfrastructure.getNodeById(currTask.getUserId()))
                )
                    target = (ComputationalNode)
                            currentInfrastructure.getNodeById(currTask.getUserId());
            } else {
                UniformIntegerDistribution uid = new
                        UniformIntegerDistribution(0,currentInfrastructure.getAllNodes().size()-1);
                ComputationalNode tmpTarget = (ComputationalNode) currentInfrastructure.getAllNodes().toArray()[uid.sample()];
                if(isValid(scheduling,currTask,tmpTarget)) {
                    target = tmpTarget;
                }
            }

            if(target!=null) {
                deploy(scheduling, currTask, target);
            }

        }


        return schedulings;
    }
}
