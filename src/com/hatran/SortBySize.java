package com.hatran;
import java.util.Comparator;
import java.util.List;


class SortBySize implements Comparator<List<Integer>>{

    public int compare(List<Integer> list1, List<Integer> list2){

        return list2.size()-list1.size();

    }

}
