package at.ac.tuwien.ec.scheduling.offloading.algorithms.etf.utils;

import java.util.Comparator;

import at.ac.tuwien.ec.model.software.MobileSoftwareComponent;

// IMPLEMENTED BY ESZTER HORVÁTH 12042300

public class NodeRankComparator implements Comparator<MobileSoftwareComponent> {

    @Override
    public int compare(MobileSoftwareComponent o1, MobileSoftwareComponent o2) {
        return Double.compare(o2.getRank(), o1.getRank()) * -1;
    }

}
