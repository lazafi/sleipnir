package lazafi.dic2021.exc3;

import at.ac.tuwien.ec.model.software.MobileSoftwareComponent;

import java.util.Comparator;

public class DLComparator implements Comparator<MobileSoftwareComponent> {

	@Override
	public int compare(MobileSoftwareComponent o1, MobileSoftwareComponent o2) {
		return Double.compare(o1.getRunTime(),o2.getRunTime());
	}

}
