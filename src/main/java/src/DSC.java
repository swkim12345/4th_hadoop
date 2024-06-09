package src;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class DSC {

    /**
     * Preprocessing
     * 실제로 hadoop상에서 제공하는 맵리듀스 라이브러리를 사용하지 않고 처리를 진행함.
     * 1. 먼저 다트의 파일을 읽고, 호재인지 악재인지 저장 + 기업 코드를 저장하게 됨.
     * 2. 기업 코드를 바탕으로 폴더에 접근해 처리를 진행함.
     * 2-a.만약, 앞 뒤 60분을 한 시간이 9시 ~ 15시 20분 시간을 초과하게 된다면, 전날 혹은 다음 날의 데이터를 바탕으로 처리함.
     * 3. 각각의 변동률, 수익률을 바로 전 분과 비교해 처리한다.
     * 4. 다트의 호재 악재를 저장하고, (기업코드_0).csv 형식으로 저장한다.
     * 4-a. 만약 0이 존재한다면 _1, _2...등등으로 증가시키면서 저장한다.
     */
    public static void preprocess(String inputFolder, String dartFolder, String outputFolder)
            throws IOException {

        Configuration conf = new Configuration();
        //hadoop filesystem에 접근하는 팩토리 메서드
        FileSystem fs = FileSystem.get(conf);

        //폴더를 읽어 처리하는 파일
        Path inFolder = new Path(inputFolder);
        Path daFolder = new Path(dartFolder);
        Path outFolder = new Path(outputFolder);

        FSDataOutputStream outputStream = fs.create(outFolder);
        if (fs.exists(daFolder)) {
            FileStatus[] fileStatus = fs.listStatus(daFolder);
            for (FileStatus status : fileStatus) {
                //daFolder에 존재하는 파일을 읽고 처리하게 됨.
                Path dartFile = status.getPath();
                if (!status.isDirectory()) {
                    FSDataInputStream inputStream = null;
                    BufferedReader br = null;
                    try {
                        inputStream = fs.open(dartFile);
                        br = new BufferedReader(new InputStreamReader(inputStream));
                        String line;
                        while ((line = br.readLine()) != null) {
                            System.out.println(line);
                            outputStream.writeBytes(line);
                        }
                    } finally {
                        IOUtils.closeStream(br);
                        IOUtils.closeStream(inputStream);
                    }
                }
            }
        }
        outputStream.close();
        fs.close();
    }

    /**
     * Mapper 상속 후 제너럴 클래스 타입 결정
     * 파일 구조 :
     * (호재 - 1, 악재 - 0)
     * (csv파일 형식으로 되어 있는 60줄짜리 파일 형식)
     * 엔터가 두번 나오기 전까지 각 줄은 dart에서 가져온 전처리된 자료
     * 이후 시간이작성되어 있는 csv파일 형식을 따름
     * MyMapper : 하나의 엔터당 하나의 Context가 나오게 됨.
     * 전체 파일을 스캔하면서 context에 하나씩 작성.
     * key : company code
     * value : 0: 30분 이내의 데이터를 시간정보와 함께 직렬화, 구분은 :으로
     *  1: 60분 이내의 데이터를 시간 정보와 함께 직렬화.
     * 1.
     */
//        public static class MyMapper
//                extends Mapper<Object, Text, Text, Text> {
//            private Text word = new Text();
//
//            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//
//            }
//        }

    /**
     * Reducer
     * Context : 호재, 악재가 key값, value의 경우 공시가 발행된 앞 뒤 60분간격의  csv파일이 한 value마다 작성되어 있음.
     * 각각의 줄을 모으고, 60 ~ 30 / 30 ~ 0 ~ 30 / 30 ~ 60까지의 데이터로 전처리함.
     * 이 줄들을 분석한 다음, 변동률, 수익률을 가지고 나이브한 형식으로 value에 작성함.
     * output : key : dart에서 분석한 호재, 악재, value : 주식 가격의 변동률, 수익률, 얼마나 작용한지
     */
//        public static class MyReducer
//                extends Reducer<Text, Text, Text, Text> {
//            protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//
//            }
//
//            /**
//             * Afterprocessing
//             * 1. 구현하지 않는다.
//             */
//        }

    public static void main(String[] args) throws Exception {
        String inputFolder = args[0];
        String dartFolder = args[1];
        String outputFolder = args[2];

        Configuration conf = new Configuration();

//            Job job = Job.getInstance(conf, "preprocess");
//
//            job.setJarByClass(preprocess.class);

        preprocess(inputFolder, dartFolder, outputFolder);
    }

}