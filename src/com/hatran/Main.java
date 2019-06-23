package com.hatran;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import com.rits.cloning.Cloner;
import org.apache.log4j.*;


public class Main {

    private final static Logger logger = Logger.getLogger(Main.class);  // use log4j to save output message

    /**
     * Get difference elements between two sets
     * @param list1
     * @param list2
     * @return
     */
    public static List<PositionInSpace> polygonCombine(List<PositionInSpace> list1, List<PositionInSpace> list2){
        //This function is find the uncommon elements from two sets
        Set<PositionInSpace> set1 = new HashSet<>(list1); // convert ArrayList to HashMap
        Set<PositionInSpace> set2 = new HashSet<>(list2);
        Set<PositionInSpace> result = new HashSet<>();  // Store the result

        for (PositionInSpace el: set1){
            if (!set2.contains(el)){
                result.add(el);
            }
        }
        for (PositionInSpace el: set2){
            if (!set1.contains(el)){
                result.add(el);
            }
        }

        List<PositionInSpace> finalResult = new ArrayList<>(result);
        return finalResult;
    }


    /**
     * Check if arraylist contain another arraylist (If arrrayList2 is contained by arrayList1)
     * @param list1
     * @param list2
     * @return
     */
    public static boolean ArrayListContainArrayList(List<Integer> list1, List<Integer> list2){
        Set<Integer> hashSet1 = new HashSet<>(list1);
        Set<Integer> hashSet2 = new HashSet<>(list2);
        boolean contain = true;
        for (Integer i: hashSet2){
            if (!hashSet1.contains(i)){
                contain = false;
                break;
            }
        }
        return contain;
    }


    /**
     * This function is update the table instances when put in a new row instance
     * @param oldTableInstances
     * @param rowInstancesList
     * @return
     */

    public static Map<List<Integer>, List<Set<Integer>>> UpdateTableInstances(
            Map<List<Integer>, List<Set<Integer>>> oldTableInstances,
            List<List<PositionInSpace>> rowInstancesList ) {

        for (List<PositionInSpace> rowInstances : rowInstancesList) {
            // From instance build a key and value
            List<Integer> key = rowInstances.stream().map(point -> point.getFeature()).collect(Collectors.toList());
            Collections.sort(key);
            int lenghRowInstance = rowInstances.size();
            if (oldTableInstances.isEmpty()) {
                List<Set<Integer>> emptyValue = new ArrayList<>();
                for (int indexEmpty = 0; indexEmpty < lenghRowInstance; indexEmpty++) {
                    Set<Integer> emptTemp = new HashSet<>();
                    emptTemp.add(rowInstances.get(indexEmpty).getInstance());
                    emptyValue.add(emptTemp);
                }
                oldTableInstances.put(key, emptyValue);
            } else {
                // table instances is not empty
                if (oldTableInstances.containsKey(key)) {
                    // key has already existed
                    List<Set<Integer>> oldValue = oldTableInstances.get(key);

                    for (int indexExisted = 0; indexExisted < lenghRowInstance; indexExisted++) {
                        oldValue.get(indexExisted).add(rowInstances.get(indexExisted).getInstance());
                    }

                    oldTableInstances.put(key, oldValue);

                } else {
                    // key has not already existed
                    List<Set<Integer>> newValue = new ArrayList<>();
                    for (PositionInSpace point : rowInstances) {
                        Set<Integer> newTemp = new HashSet<>();
                        newTemp.add(point.getInstance());
                        newValue.add(newTemp);
                    }
                    oldTableInstances.put(key, newValue);

                }
            }
        }
        return oldTableInstances;
    }



    public static void main(String[] args){

        long start = System.currentTimeMillis(); // Count run of time
        Cloner cloner = new Cloner(); // use to clone an object

        logger.info("This is a program which using Delaunay triangulation for mining Co-location patterns!");

        Float PIThreshold = 0.05f;  // participation index

//         First: Load star Delaunay Triangle from json file to a HashMap
//        String json_file = "./data/normal_syntheticdata/nomarl_20k_35_4kx4k_StarDelaunayTriangle_json.json";
        String json_file = "./data/realdata/LasVegas_delete_feature_StarDelaunayTriangle_json.json";

        // Load the number of instances corresponding features to a HashMap
//        String numberInstanceJsonFile = "./data/normal_syntheticdata/number_of_instance_nomarl_20k_35_4kx4k.json";
        String numberInstanceJsonFile = "./data/realdata/LasVegas_number_of_instance.json";

        // Save results
//        String result = "./out/normal_syntheticdata/nomarl_20k_35_4kx4k_pi006_RRClosed_result.json";
//        String resultSize2 ="./out/normal_syntheticdata/nomarl_20k_35_4kx4k_pi006_size2.json";
//        String resultSize3 ="./out/normal_syntheticdata/nomarl_20k_35_4kx4k_pi006_size3.json";

        String result = "./out/realdata/LasVegas_result.json";
        String resultSize2 ="./out/realdata/LasVegas_result_size_2.json";
        String resultSize3 ="./out/realdata/LasVegas_result_size_3.json";


        HashMap<Integer, List<List<PositionInSpace>>> starDelaunayTriangle = new HashMap<>(); // Store star delaunay triangle
        try{
            ObjectMapper mapper = new ObjectMapper();
            // Read JSON from a file
            starDelaunayTriangle = mapper.readValue(
                    new File(json_file),
                    new TypeReference<HashMap<Integer, List<List<PositionInSpace>>>>(){}
            );

        } catch (IOException e){
            e.printStackTrace();
        }

        //  Second: Combine Delaunay Triangle to build a large Polygon
        Map<List<Integer>, List<Set<Integer>>> tableInstances = new HashMap<>(); // Store all table instances in a HashMap,
        // e.g. <"abc": [(1,2), (1,2,3), (4,5)]>, (1,2) is the a of instances, (1,2,3) is the b of instances, (4,5) is the c of instances

        // 1. Loop HashMap
        Iterator<Map.Entry<Integer, List<List<PositionInSpace>>>> itr = starDelaunayTriangle.entrySet().iterator();
        while (itr.hasNext()){
            Map.Entry<Integer, List<List<PositionInSpace>>> entry = itr.next();
            List<List<PositionInSpace>> valuePolygon = entry.getValue();
            while (valuePolygon.size() >= 1) {
                if (valuePolygon.size() == 1) { // If has only one triangle (polygon), do not combine, put it into table instances directly
                    // Step1: Generate the size 2:k row instance and put in table instances
                    int lengthRowInstance = valuePolygon.get(0).size();
                    for(int iOneRowInstance = 2; iOneRowInstance <= lengthRowInstance; iOneRowInstance++){
                        tableInstances = UpdateTableInstances(tableInstances, CombinationGenerator.findsort(valuePolygon.get(0), iOneRowInstance));
                    }
                    break; // end of loop, go to next valuePolygon
                } else { // has larger than 2 polygon -> combine
                    List<List<PositionInSpace>> starPolygon = new ArrayList<>(); //store all polygon which its size is larger than > 4
                    // Only deal with value.size is bigger than 2
                    // Clone the v
                    List<List<PositionInSpace>> vClone = cloner.deepClone(valuePolygon);
                    for( List<PositionInSpace> rowInstance: valuePolygon){
                        vClone.remove(rowInstance); // delete the need check row instances
                        if (vClone.isEmpty()){ // loop until the end of element
                            int sizeEmptyVClone = rowInstance.size();
                            for(int indexEmptyVClone = 2; indexEmptyVClone <= sizeEmptyVClone; indexEmptyVClone++){
                                tableInstances = UpdateTableInstances(tableInstances, CombinationGenerator.findsort(rowInstance, indexEmptyVClone));
                            }
                            break;

                        }else {
                            // go to combine polygons
                            List<List<PositionInSpace>> temp = vClone.stream().map(checkTnstance ->{
                                // combine and check rowInstacne combine to other in cClone
                                List<PositionInSpace> uncommentElementPolygon = polygonCombine( rowInstance, checkTnstance);
                                // check combine result, if successful combining
                                if ((uncommentElementPolygon.size() == 2)
                                        && (uncommentElementPolygon.get(0).getFeature() != uncommentElementPolygon.get(1).getFeature())){
                                    // put rowInstnace to build a larger polygon
                                    uncommentElementPolygon.addAll(rowInstance);
                                    // delete the duplicate
                                    Set<PositionInSpace> setDuplicateTempPolygon = new HashSet<>(uncommentElementPolygon);
                                    // convert back ArrayList
                                    uncommentElementPolygon.clear();
                                    uncommentElementPolygon.addAll(setDuplicateTempPolygon);
                                    // sort
                                    Collections.sort(uncommentElementPolygon, new SortByFeature());
                                    return uncommentElementPolygon;
                                }else {

                                    return rowInstance; // unsuccessful combine, return its own
                                }
                            }).filter(newRow -> !newRow.equals(rowInstance))
                                    .collect(Collectors.toList());
                            if (temp.isEmpty()){ // unsuccessful combine a larger polygon
                                // put into table instance and end the loop
                                int sizeUnSucCombineRowIns = rowInstance.size();
                                for (int indexUnsucCombine = 2; indexUnsucCombine<= sizeUnSucCombineRowIns; indexUnsucCombine++){
                                    tableInstances = UpdateTableInstances(tableInstances, CombinationGenerator.findsort(rowInstance, indexUnsucCombine));
                                }
                                break;
                            }else { // successful combine to a larger polygon
                                starPolygon.addAll(temp);
                            }
                        }
                    }
                    // Delete the dunplicate elements in starPolygon
                    Set<List<PositionInSpace>> starPolygonSet = new HashSet<>(starPolygon);
                    // Convert back to ArrayList
                    starPolygon.clear();
                    starPolygon.addAll(starPolygonSet);
                    // Point the pointer to starPolygon to control
                    valuePolygon = starPolygon;
                }
            }
        }
        // Third: Load the instance numbers file
        Map<Integer, Float> numberInstances = new HashMap<>();
        try{
            ObjectMapper mapperInstance = new ObjectMapper();
            // Read JSON from a file
            numberInstances = mapperInstance.readValue(
                    new File(numberInstanceJsonFile),
                    new TypeReference<Map<Integer, Float>>(){}
            );
        } catch (IOException e){
            e.printStackTrace();
        }

        // Fourth: Filter co-location patterns
        // Compute the PI of patterns
        Map<List<Integer>, Float> pis = new HashMap<>(); // store the PI of patterns
        // Loop table instances
        Iterator<Map.Entry<List<Integer>, List<Set<Integer>>>> tableItr = tableInstances.entrySet().iterator();
        while (tableItr.hasNext()){
            Map.Entry<List<Integer>, List<Set<Integer>>> tableEntry = tableItr.next();
            // store a pattern of prs
            List<Float> PR = new ArrayList<>();
            int sizek = tableEntry.getKey().size();
            for (int indexSizek = 0; indexSizek<sizek; indexSizek++){
                // Get the number of the feature
                Float numInstance = numberInstances.get(tableEntry.getKey().get(indexSizek));
                // Get the number of instances of the pattern
                Integer numTableInstance = tableEntry.getValue().get(indexSizek).size();
                // compute pr
                PR.add(numTableInstance/numInstance);
            }
            // pi
            if (Collections.min(PR) >= PIThreshold){
                pis.put(tableEntry.getKey(), Collections.min(PR));
            }
        }
        // Fifth: Filter closed patterns
        // First: group pattern if the pi value is the same
        Map<Float, List<List<Integer>>> samePIPatterns = new HashMap<>();
        pis.forEach((keyPI, valuePI)->{
            if (samePIPatterns.isEmpty()){
                List<List<Integer>> emptyValue = new ArrayList<>();
                emptyValue.add(keyPI);
                samePIPatterns.put(valuePI,emptyValue);
            }else {
                if(samePIPatterns.containsKey(valuePI)){
                    List<List<Integer>> oldValuePattern = new ArrayList<>(samePIPatterns.get(valuePI));
                    oldValuePattern.add(keyPI);
                    samePIPatterns.put(valuePI, oldValuePattern);
                }else {
                    List<List<Integer>> newValue = new ArrayList<>();
                    newValue.add(keyPI);
                    samePIPatterns.put(valuePI, newValue);
                }
            }
        });

        // Second: check if a pattern is a supper set of other patterns
        Map<Float, List<List<Integer>>> closedPatterns = new HashMap<>();
        samePIPatterns.forEach((sameKey, sameValue)->{
            Collections.sort(sameValue, new SortBySize());
            List<List<Integer>> copyClosedPatterns = new ArrayList<>(sameValue);
            int sizeOfSameValue = sameValue.size();
            for (int i=0; i< sizeOfSameValue-1; i++){
                for(int j=i+1; j<sizeOfSameValue; j++){
                    boolean contain = ArrayListContainArrayList(sameValue.get(i), sameValue.get(j));
                    if (contain){
                        copyClosedPatterns.remove(sameValue.get(j));
                    }
                }
            }
            closedPatterns.put(sameKey, copyClosedPatterns);
        });

        // Sixth: Filter RRClosed patterns
        // First: Put all closed pattern together
        List<List<Integer>> allClosedPatterns = new ArrayList<>();
        closedPatterns.forEach((closedKey, closedValue)->{
            allClosedPatterns.addAll(closedValue);
        });
        // Sort
        Collections.sort(allClosedPatterns, new SortBySizeDescending());
        // Second: Check the closed super patterns set:
        Map<List<Integer>, Float> rrClosedPatterns = new HashMap<>(); // store the final results
        for(int index = 0; index < allClosedPatterns.size(); index++){
            Set<List<Integer>> superClosedSet = new HashSet<>(); // Store all super closed patterns
            List<Integer> checkPattern = new ArrayList<>(allClosedPatterns.get(index));
            for (int indexInner = index+1; indexInner<allClosedPatterns.size(); indexInner ++ ){
                // check if it is the subset
                boolean isSubset = ArrayListContainArrayList(allClosedPatterns.get(indexInner), checkPattern);
                int directlySubset = allClosedPatterns.get(indexInner).size()-checkPattern.size();
                if (isSubset && directlySubset == 1){
                    superClosedSet.add(allClosedPatterns.get(indexInner));
                }
            }

            if (superClosedSet.isEmpty()){
                // this pattern is a hard pattern
                rrClosedPatterns.put(checkPattern, pis.get(checkPattern));

            }else {
                // This pattern has some subset patterns
                // Call ESD according to equal 5
                List<Float> ESD = new ArrayList<>(); //Store the ESD of a pattern
                for (int ifeaturePattern = 0; ifeaturePattern< checkPattern.size(); ifeaturePattern++){
                    // Take the number of instance in table instance of checkParttern
                    Integer numberInstanceCheckPattern = tableInstances.get(checkPattern).get(ifeaturePattern).size();
                    Set<Integer> combineNumberInstanceSuperSet = new HashSet<>();
                    for(List<Integer> superSetOfCheckPattern: superClosedSet){
                        int indexOfFeatureinSuperSet = superSetOfCheckPattern.indexOf(checkPattern.get(ifeaturePattern));
                        combineNumberInstanceSuperSet.addAll(tableInstances.get(superSetOfCheckPattern).get(indexOfFeatureinSuperSet));
                    }
                    ESD.add(1f - combineNumberInstanceSuperSet.size()/numberInstanceCheckPattern);
                }
                if (Collections.min(ESD) != 0){
                    // save this pattern
                    rrClosedPatterns.put(checkPattern, pis.get(checkPattern));
                }
            }
        }
        // sort the RRClosed by pi
        Map<List<Integer>, Float> sortedPIRRClosed = rrClosedPatterns
                .entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2)->e1, LinkedHashMap::new));

        // Save the final result to txt file
        ObjectMapper mapper = new ObjectMapper();
        try {
            mapper.writeValue(new File(result), sortedPIRRClosed);
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("The total number of patterns is:"+sortedPIRRClosed.size());

        // Filter maximal size pattern
        List<Integer> allSize = sortedPIRRClosed.entrySet().stream().map(pattern->pattern.getKey().size()).collect(Collectors.toList());
        Set<Integer> allSizeSet = new HashSet<>(allSize);
        logger.info("All size patterns: "+ allSizeSet);
        // Count each size of patterns
        Map<Integer, Integer> countSizek = new HashMap<>();
        allSizeSet.forEach(sizeK -> {
            Map<List<Integer>, Float> tempSizek = sortedPIRRClosed.entrySet()
                    .stream()
                    .filter(patt->patt.getKey().size()==sizeK)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2)->e1, LinkedHashMap::new));
            countSizek.put(sizeK, tempSizek.size());
        });
        logger.info("The number of each size patterns is: " + countSizek);

        // Statistic the size number patterns
        // Size 2
        Map<List<Integer>, Float> size2 = sortedPIRRClosed.entrySet().stream().
                filter( pattern -> pattern.getKey().size()==2 )
                .collect(Collectors.toMap(pt -> pt.getKey(),pt-> pt.getValue()));
        // Sorted size 3 patterns as pi
        Map<List<Integer>, Float> sortedSize2 = size2
                .entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2)->e1, LinkedHashMap::new));

        logger.info("Number of size 2: " + sortedSize2.size());
        logger.info(sortedSize2);

        // Size 3
        Map<List<Integer>, Float> size3 = sortedPIRRClosed.entrySet().stream().
                filter( pattern -> pattern.getKey().size() == 3)
                .collect(Collectors.toMap(pt -> pt.getKey(),pt-> pt.getValue()));
        // Sorted size 3 patterns as pi
        Map<List<Integer>, Float> sortedSize3 = size3
                .entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2)->e1, LinkedHashMap::new));
        logger.info("Number of size 3: " + sortedSize3.size());
        logger.info(sortedSize3);

        // save to file
        try {
            mapper.writeValue(new File(resultSize2), sortedSize2);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            mapper.writeValue(new File(resultSize3), sortedSize3);
        } catch (IOException e) {
            e.printStackTrace();
        }

        logger.info("Program end!");
        long end = System.currentTimeMillis();
        logger.info("Total time：" + (end - start) / 1000.0  + " seconds");
    }

}

