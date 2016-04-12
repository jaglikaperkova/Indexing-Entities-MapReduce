package projet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import com.google.common.collect.Multimap;
import com.google.common.collect.ArrayListMultimap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import scala.Array;

public class App {

    public static Multimap<String, String> multiMap = ArrayListMultimap.create();

    public static void reverse(String s1, String s2) {
        String help;
        help = s1;
        s1 = s2;
        s2 = s1;
    }

    //e0 devient representant de e1 c-à-d le representant e1 devient representé par e0

    public static void caseRptRpt(String e0, String e1, Multimap<String, String> multiM) {

        multiM.put(e0, e1);
        Collection values;
        //marquer e0 comme representant de e1
        values = multiM.get(e1);
        for (Iterator<String> iterator = values.iterator(); iterator.hasNext();) {
            String element = iterator.next();
            //marquer les representées par e1 comme represetées par e0
            if (!multiM.get(e0).contains(element)) {
                multiM.put(e0, element);
            }
        }
        //supprimer e1 des representants avec ses representées
        multiM.removeAll(e1);

    }

    public static boolean removeValueOfKey(String key, String value, Multimap<String, String> multiM) {
        Collection values = multiM.get(key);
        for (Iterator<String> iterator = values.iterator(); iterator.hasNext();) {
            String element = iterator.next();
            if (value.equals(element)) {
                values.remove(element);
               // if (values.isEmpty()){
                //    multiM.removeAll(key);
                //}
                return true;
            }

        }
        return false;
    }
    
    public static String getKeyOfValue(String value, Multimap<String, String> multiM) {
        String keyOfValue = new String();
        Set<String> setKeys = multiM.keySet();
        for (String key : setKeys) {
            if (multiM.get(key).contains(value)) {
                keyOfValue = key;

            }
        }
        return keyOfValue;
    }
    public static void caseRptRpte(String rpt, String rpte, Multimap<String, String> multiM) {
        //marquer rpt comme representé par le representant de rpte
        multiM.put(getKeyOfValue(rpte, multiM), rpt);
        Collection values;
        values = multiM.get(rpt);
        for (Iterator<String> iterator = values.iterator(); iterator.hasNext();) {
            String element = iterator.next();
            //marquer les representées par e0 comme represetées par e1
            multiM.put(getKeyOfValue(rpte, multiM), element);
        }
        multiM.removeAll(rpt);
    }
    public static boolean findRepresenteInList(List<Tuple2<String, String>> list, String el){
       
        for(Iterator<Tuple2<String,String>> iterator = list.iterator(); iterator.hasNext();){
            Tuple2<String, String> pair = iterator.next();
            if(pair._2.equals(el)){
                return true;
            }
        }
        return false;
    }
    public static void filterEntities(List<Tuple2<String, String>> l, Multimap<String, String> multiM){
        boolean t1;
        for(int i=0;i<l.size();i++){
     //       System.out.println(i);
            String s1 = l.get(i)._1;
            String s2 = l.get(i)._2;
       //     System.out.println("s1= "+s1+" s2= "+s2);
      //      System.out.print("i= ");
      //      System.out.println(i);
      //      System.out.print("size()=  ");
      //      System.out.println(l.size());
            t1=findRepresenteInList(l,s1);
       //     System.out.println("findRepresenteInList(l,s1): "+s1+" = "+t1);
            //t2=findElementInList(l,l.get(i)._2);
            if(!multiMap.containsEntry(s1, s2)){
                if(multiM.containsValue(s1)){
                    if(!t1){
             //           System.out.println(l.size());
                        l.remove(i);
                        i--;
                        l.add(new Tuple2<>(getKeyOfValue(s1,multiM),s1));
            //            System.out.println(l.size());
                        //l.remove(i);
                        
                    }
                    else{
              //          System.out.println(l.size());
                        l.remove(i);
                        i--;
              //          System.out.println(l.size());
                        l.add(new Tuple2<>(getKeyOfValue(s2,multiM),s2));
             //           System.out.println(l.size());
                        
                    }
                }
                else{
                    l.remove(i);
             //       System.out.println(l.size());
                    i--;
                }
            }

        }
    }
    private static final FlatMapFunction<String, String> WORDS_EXTRACTOR
            = new FlatMapFunction<String, String>() {
                @Override
                public Iterable<String> call(String s) throws Exception {

                    return Arrays.asList(s.split("\n"));

                }
            };

    private static final PairFunction<String, String, String> MAPPER
            = new PairFunction<String, String, String>() {
                @Override
                public Tuple2<String, String> call(String s) throws Exception {
                    String representant="";
                    String representé="";
                    String[] tabLine;
                    tabLine = s.split(";");
                    //si multiMap est vide
                    if (multiMap.isEmpty()) {
                        //on marque tout de suite tabLine[0] representant et tabLine[1] representé
                        multiMap.put(tabLine[0], tabLine[1]);
                        representant=tabLine[0];
                        representé=tabLine[1];
                    }//sinon si multiMap n'est pas vide
                    else if (!multiMap.isEmpty()) {
                //---------BEGIN CASE : REPRESENT-----------
                        //si tabLine[0] est representant ou tabLine[1] est representant
                        if (multiMap.containsKey(tabLine[0]) || multiMap.containsKey(tabLine[1])) {
                            //Cas 1: tabLine[0] étant representant
                            if (multiMap.containsKey(tabLine[0])) {
                                //si le couple clé-valeur existe déjà
                                if (multiMap.containsEntry(tabLine[0], tabLine[1])) {
                            //on ne traite pas ce cas donc on renvoie rien, ici des String-s vides
                                    //return new Tuple2<String, String>("","");
                                    //A VERIFIER SI CETTE ECRITURE PASSE OU PAS !!!!
                                    representant = "";//<- to verify
                                    representé = "";//<- to verify
                                } //sinon si tabLine[1] est aussi representant
                                else if (multiMap.containsKey(tabLine[1])) {
                                    //marquer tabLine[1] comme representant de tabLine[0] 
                                    //marquer les representées par tabLine[0] comme representées de tabLine[1] 
                                    caseRptRpt(tabLine[1], tabLine[0], multiMap);
                                    //return new Tuple2<String, String>(tabLine[1],tabLine[0]);
                                    representant = tabLine[1];
                                    representé = tabLine[0];
                                } //sinon si tabLine[1] est representé
                                else if (multiMap.containsValue(tabLine[1])) {
                                    //supprimer tabLine[1] des representées par l'entité ancienne representante
                                    removeValueOfKey(getKeyOfValue(tabLine[1], multiMap), tabLine[1], multiMap);
                                    //marquer tabLine[1] comme representé par tabLine[0]
                                    multiMap.put(tabLine[0], tabLine[1]);
                                    //return new Tuple2<String, String>(tabLine[0], tabLine[1]);
                                    representant=tabLine[0]; 
                                    representé=tabLine[1];
                                }
                                else{//sinon
                                    multiMap.put(tabLine[0], tabLine[1]);
                                    //return new Tuple2<String, String>(tabLine[0], tabLine[1]);
                                    representant = tabLine[0];
                                    representé = tabLine[1];
                                }
                            } //Cas 2: tabLine[1] étant representant
                            else if (multiMap.containsKey(tabLine[1])) {
                                //si le couple clé-valeur existe déjà
                                if(multiMap.containsEntry(tabLine[1], tabLine[0])){
                                    //on ne traite pas ce cas donc on renvoie rien, ici des String-s vides
                                    //return new Tuple2<String, String>("","");
                                    //A VERIFIER SI CETTE ECRITURE PASSE OU PAS !!!!
                                    representant = "";//<- to verify
                                    representé = "";//<- to verify
                                }//sinon si tabLine[0] est representant aussi
                                else if(multiMap.containsKey(tabLine[0])){
                                    //marquer tabLine[0] representant de tabLine[1] 
                                    //marquer les representées par tabLine[0] comme des representées par tabLine[1]
                                    caseRptRpt(tabLine[0], tabLine[1], multiMap);
                                    //return new Tuple2<String, String>(tabLine[0], tabLine[1]);
                                    representant = tabLine[0];
                                    representé = tabLine[1];
                                }
                                //sinon si tabLine[1] est representé
                                else if(multiMap.containsValue(tabLine[0])){
                                    //supprimer tabLine[0] de la liste des representées par l'ancienne entité representante
                                    removeValueOfKey(getKeyOfValue(tabLine[0], multiMap), tabLine[0], multiMap);
                                    //marquer tabLine[1] comme representant de tabLine[0]
                                    multiMap.put(tabLine[1], tabLine[0]);
                                    //return new Tuple2<String, String>(tabLine[1], tabLine[0]);
                                    representant = tabLine[1];
                                    representé = tabLine[0];
                                }else{//sinon
                                    //marquer tabLine[0] comme representé par tabLine[1]
                                    multiMap.put(tabLine[1], tabLine[0]);
                                    //return new Tuple2<String, String>(tabLine[1], tabLine[0]);
                                    representant = tabLine[1];
                                    representé = tabLine[0];
                                }
                            }
                        } //--------BEGIN CASE : REPRESENTED-----------
                        //si tabLine[0] est representé ou tabLine[1] est representé
                        else if (multiMap.containsValue(tabLine[0]) || multiMap.containsValue(tabLine[1])) {
                            //Cas 3: tabLine[0] est representé
                            if (multiMap.containsValue(tabLine[0])) {
                                //si le couple clé-valeur (tabLine[1], tabLine[0]) existe déjà ou dans le cas où les deux entités sont  representées par un même entité representant 
                                if(multiMap.containsEntry(tabLine[1], tabLine[0]) || getKeyOfValue(tabLine[0], multiMap).equals(getKeyOfValue(tabLine[1], multiMap))){
                                    //on fait rien
                                    //return new Tuple2<String, String>("","");
                                    representant = "";
                                    representé = "";
                                }
                                //sinon si tabLine[1] est representant
                                //else if(multiMap.containsKey(tabLine[1])){
                                    //marquer tabLine[1] comme representé par le representant de tabLine[0]
                                    //marquer les representées par tabLine[1] comme des representées par le representant de tabLine[0]
                                   // caseRptRpte(tabLine[1], tabLine[0], multiMap);
                                    //return new Tuple2<String, String>(getKeyOfValue(tabLine[0], multiMap), tabLine[1]);
                                   // representant = getKeyOfValue(tabLine[0],multiMap);
                                  //  representé = tabLine[1];
                                    //ou essayer seulement de faire marquer tabLine[0] comme representé par tabLine[1] et de supprimer tabLine[0] de l'ancien representant
                                //}
                                //si tabLine[1] est aussi representé mais par une autre entité representant
                                else if(multiMap.containsValue(tabLine[1])){                                    
                                    String val=getKeyOfValue(tabLine[0], multiMap);                                    
                                    //supprimer tabLine[0] de son ancien representant
                                    removeValueOfKey(getKeyOfValue(tabLine[0], multiMap), tabLine[0], multiMap);
                                    //marquer tabLine[0] comme representé par le representant du tabLine[1]
                                    multiMap.put(getKeyOfValue(tabLine[1], multiMap), tabLine[0]);
                                    Collection v =multiMap.get(val);
                                    if(v.size()==0){
                                        multiMap.removeAll(val);
                                        multiMap.put(getKeyOfValue(tabLine[1], multiMap), val);    
                                    }
                                    //return new Tuple2<String, String>(getKeyOfValue(tabLine[1], multiMap),tabLine[0]);
                                    representant = getKeyOfValue(tabLine[1], multiMap);
                                    representé = tabLine[0];
                                }
                                else{
                                    //sinon marquer tabLine[1] comme representé par le representant de tabLine[0]
                                    multiMap.put(getKeyOfValue(tabLine[0], multiMap), tabLine[1]);
                                    //return new Tuple2<String, String>(getKeyOfValue(tabLine[0],multiMap),tabLine[1]);
                                    representant = getKeyOfValue(tabLine[0],multiMap);
                                    representé = tabLine[1];
                                }
                            } //Cas 4 : tabLine[1] est representé
                            else if (multiMap.containsValue(tabLine[1])) {
                                //si le couple clé-valeur (tabLine[0], tabLine[1]) existe déjà ou dans le cas où les deux entités sont  representées par un même entité representant
                                if(multiMap.containsEntry(tabLine[0], tabLine[1]) || getKeyOfValue(tabLine[1], multiMap).equals(getKeyOfValue(tabLine[0], multiMap))){
                                    //return new Tuple2<String, String>("","");
                                    representant = "";
                                    representé = "";
                                }
                                //sinon si tabLine[0] est representant
                                //else if(multiMap.containsKey(tabLine[0])){
                                    //marquer tabLine[0] comme representé par le representant de tabLine[1]
                                    //marquer toutes les representées par tabLine[0] comme des representées par le representant de tabLine[1]
                                  //  caseRptRpte(tabLine[0], tabLine[1], multiMap);
                                    //return new Tuple2<String, String>(getKeyOfValue(tabLine[1], multiMap),tabLine[0]);
                                  //  representant = getKeyOfValue(tabLine[1], multiMap);
                                  //  representé = tabLine[0];
                                //}
                                //sinon si tabLine[0] est aussi representée par un representant diff de celui de tabLine[1]
                                else if(multiMap.containsValue(tabLine[0])){
                                    String val=getKeyOfValue(tabLine[1], multiMap);  
                                    //supprimer tabLine[1] des representées par son ancien representant
                                    removeValueOfKey(getKeyOfValue(tabLine[1], multiMap), tabLine[1], multiMap);
                                    //marquer tabLine[1] comme representé par le representant de tabLine[0]
                                    multiMap.put(getKeyOfValue(tabLine[0], multiMap), tabLine[1]);
                                    Collection v =multiMap.get(val);
                                    if(v.size()==0){
                                        multiMap.removeAll(val);
                                        multiMap.put(getKeyOfValue(tabLine[0], multiMap), val);    
                                    }
                                    //return new Tuple2<String, String>(getKeyOfValue(tabLine[0], multiMap), tabLine[1]);
                                    representant = getKeyOfValue(tabLine[0], multiMap);
                                    representé = tabLine[1];
                                }
                                //sinon, marquer tabLine[0] comme representé par le representant de tabLine[1]
                                else{
                                    multiMap.put(getKeyOfValue(tabLine[1], multiMap), tabLine[0]);
                                    //return new Tuple2<String, String>(getKeyOfValue(tabLine[1], multiMap), tabLine[0]);
                                    representant = getKeyOfValue(tabLine[1], multiMap);
                                    representé = tabLine[0];
                                }
                            }
                        } //sinon
                        else if(!tabLine[0].equals(tabLine[1])){
                            multiMap.put(tabLine[0], tabLine[1]);
                            //return new Tuple2<String, String>(tabLine[0], tabLine[1]);
                            representant = tabLine[0];
                            representé = tabLine[1];
                        }
                    }
                    //Tuple2<String. String> => le 1er String c'est toujours le representant, le 2eme le representé
                    return new Tuple2<>(representant, representé);
                }
            };


    private static final Function2<String, String, String> REDUCER
            = new Function2<String, String, String>() {
                @Override
                public String call(String a, String b) throws Exception {
                    String output;
                    
                    output = a + "," + b;
                    return output;
                }
            };

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Please provide the input file full path as argument");
            System.exit(0);
        }
        SparkConf conf = new SparkConf().setAppName("projet.App").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> file = context.textFile(args[0]);
      //  System.out.println("starting....");
        //System.out.println("File: " + file);

        JavaRDD<String> records = file.flatMap(WORDS_EXTRACTOR);
        JavaPairRDD<String, String> pairs = records.mapToPair(MAPPER);
        
       List<Tuple2<String, String>> list = pairs.collect();
        
      //  System.out.println(list);  
        //pairs.flatMapToPair(null)
        //System.out.println(multiMap);
        filterEntities(list, multiMap);
        JavaPairRDD<String,String> li = context.parallelizePairs(list);
        JavaPairRDD<String, String> counter = li.reduceByKey(REDUCER);
        System.out.println("finishing...");
        counter.saveAsTextFile(args[1]);
        context.stop();
       // JavaPairRDD<String, String> mapper1 = pairs.mapToPair(WORDS_MAPPER1);
       // mapper1.saveAsTextFile(args[1]);
       // List<Tuple2<String, String>> l1;
        
        //System.out.println(list);        
       // System.out.println(multiMap);
    }
}
