package com.hatran;

import java.util.Comparator;

class SortByFeature implements Comparator<PositionInSpace> {

    public int compare(PositionInSpace instance1, PositionInSpace instance2){
        return instance1.getFeature() - instance2.getFeature();
    }

}
