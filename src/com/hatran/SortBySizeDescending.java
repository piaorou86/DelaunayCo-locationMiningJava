package com.hatran;

import java.util.List;
import java.util.Comparator;

class SortBySizeDescending implements Comparator<List<Integer>> {

    public int compare(List<Integer> list1, List<Integer> list2) {

        return list1.size() - list2.size();

    }
}
