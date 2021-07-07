package lazafi.dic2021.exc3;

import at.ac.tuwien.ec.model.infrastructure.computationalnodes.ComputationalNode;
import at.ac.tuwien.ec.model.software.MobileSoftwareComponent;
import scala.Tuple3;

public class CandidatesComparator implements java.util.Comparator<Tuple3<MobileSoftwareComponent, ComputationalNode, Double>> {
    @Override
    public int compare(Tuple3<MobileSoftwareComponent, ComputationalNode, Double> t1, Tuple3<MobileSoftwareComponent, ComputationalNode, Double> t2) {
        return Double.compare(t1._3(), t2._3());
    }
}
