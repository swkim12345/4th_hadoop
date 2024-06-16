//package src;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;

public class checkStockVol {
    final private int period = 30;

    public static boolean isWeekday(LocalDateTime dateTime) {
        DayOfWeek dayOfWeek = dateTime.getDayOfWeek();
        return (dayOfWeek == DayOfWeek.SATURDAY || dayOfWeek == DayOfWeek.SUNDAY);
    }

    public static String getDir(String[] input)
    {
        String ret = new String();
        for (String in : input)
        {
            ret = ret.concat("/");
            ret = ret.concat(in);
        }
        return (ret);
    }

    public static Path findRightPath(String inputFolder, String stock_code, FileSystem fs, LocalDateTime now)
            throws IOException
    {
        String filename = String.format("%06d", Integer.parseInt(stock_code));
        DateTimeFormatter folder_formatter = DateTimeFormatter.ofPattern("yyyy_MM_dd");
        String date_dir = getDir(new String[] {inputFolder, now.format(folder_formatter)});
        Path kospi_path = new Path(getDir(new String[]{date_dir, "kospi", filename}) + ".csv");
        Path kosdaq_path = new Path(getDir(new String[]{date_dir, "kosdaq", filename}) + ".csv");

        if (fs.exists(kospi_path))
            return (kospi_path);
        else if (fs.exists(kosdaq_path))
            return (kosdaq_path);
        else
            return (null);
    }

    public static void inputToList(Path path, FileSystem fs, ArrayList<String> write_list, LocalDateTime start, LocalDateTime end, String hoze, String stock_code)
            throws IOException
    {
        if (path == null || !fs.exists(path))
        {
            System.out.println("skip");
            return ;
        }
        BufferedReader read_csv_br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String read_csv_line = read_csv_br.readLine();
        while ((read_csv_line = read_csv_br.readLine()) != null) {
            String time = read_csv_line.split(",")[9];
            Integer hour = Integer.parseInt(time.substring(0, 2));
            Integer minute = Integer.parseInt(time.substring(2, 4));
            LocalDateTime cmp = LocalDateTime.of(start.getYear() , start.getMonth(), start.getDayOfMonth(), hour, minute);
//            System.out.println("cmp + " + cmp);
            if (start.compareTo(cmp) <= 0 && end.compareTo(cmp) >= 0) {
                write_list.add(read_csv_line + "," + hoze + "," + stock_code);
            }
        }
    }

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

        Path daFolder = new Path(dartFolder);

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
                        //첫번째의 경우는 스킵.
                        String line = br.readLine();
                        while ((line = br.readLine()) != null) {
//                            System.out.println(line);
                            //combined_output.csv v파일 구조
                            /**
                             * 주식코드
                             * corp_code, corp_name, stock_code, report_num, rcept_no, recpt_dt, time, 호재성
                             * 실재 필요한 것은 stock_code, rcept_dt, time, 호재성(TRUE, FALSE);
                             * 0부터 시작하면 2, 5,6,7
                             */
                            String stock_code = line.split(",")[2];
                            String rcept_dt = line.split(",")[5];
                            String time = line.split(",")[6];
                            String hoze = line.split(",")[7];
                            System.out.println("stock_code : " + stock_code);
                            System.out.println("rcept_dt : " + rcept_dt);
                            System.out.println("time : " + time);
                            System.out.println("hoze : " + hoze);
                            if (stock_code.isEmpty() || rcept_dt.isEmpty() || time.isEmpty() || hoze.isEmpty()) {
                            }
                            else {
                                ArrayList<String> write_list = new ArrayList<>();
                                /**
                                 * 날짜, 시간, 파일 이름순
                                 */
                                /**
                                 * 분봉 csv파일 형식
                                 * prdy_vrss,prdy_vrss_sign,prdy_ctrt,stck_prdy_clpr,acml_vol,acml_tr_pbmn,hts_kor_isnm,stck_prpr,stck_bsop_date,stck_cntg_hour,stck_prpr,stck_oprc,stck_hgpr,stck_lwpr,cntg_vol,acml_tr_pbmn
                                 * (날짜(20230412)8, (시간 : 090000)9, (현재가)10, (1분간 거래량)14
                                 */
                                //TODO: Refactoring using format
                                String stock_code_format = String.format("%06d", Integer.parseInt(stock_code));

                                Integer hour = Integer.parseInt(time.split(":")[0]);
                                Integer minute = Integer.parseInt(time.split(":")[1]);
                                Integer year = Integer.parseInt(rcept_dt.substring(0, 4));
                                Integer month = Integer.parseInt(rcept_dt.substring(4, 6));
                                Integer day = Integer.parseInt(rcept_dt.substring(6, 8));
                                DateTimeFormatter folder_formatter = DateTimeFormatter.ofPattern("yyyy_MM_dd");
                                LocalDateTime now = LocalDateTime.of(year, month, day, hour, minute);
                                LocalDateTime prev_now = now.minusDays(1);
                                LocalDateTime future_now = now.plusDays(1);
                                while (isWeekday(prev_now)) {
                                    prev_now = prev_now.minusDays(1);
                                }
                                while (isWeekday(future_now)) {
                                    future_now = future_now.plusDays(1);
                                }

                                Path prev_path = findRightPath(inputFolder, stock_code, fs, prev_now);
                                Path now_path = findRightPath(inputFolder, stock_code, fs, now);
                                Path future_path = findRightPath(inputFolder, stock_code, fs, future_now);

                                LocalDateTime start;
                                LocalDateTime end;

                                //TODO: refactoring more efficiently
                                if (hour < 9 || (hour == 9 && minute < 30)) { //전날
                                    start = LocalDateTime.of(year, prev_now.getMonth(), prev_now.getDayOfMonth(), 14, 51);
                                    end = LocalDateTime.of(year, prev_now.getMonth(), prev_now.getDayOfMonth(), 15, 20);

                                    inputToList(prev_path, fs, write_list, start, end, hoze, stock_code_format);
                                    start = LocalDateTime.of(year, month, day, 9, 0);
                                    end = LocalDateTime.of(year, month, day, 9, 30);
                                    inputToList(now_path, fs, write_list, start, end, hoze, stock_code_format);
                                } else if (hour >= 15 || (hour == 14 && minute > 50))
                                {
                                    start = LocalDateTime.of(year, month, day, 14, 50);
                                    end = LocalDateTime.of(year, month, day, 15, 20);

//                                    //system println
                                    System.out.println(start);
                                    System.out.println(end);

                                    inputToList(now_path, fs, write_list, start, end, hoze, stock_code_format);
                                    start = LocalDateTime.of(year, future_now.getMonth(), future_now.getDayOfMonth(), 9, 0);
                                    end = LocalDateTime.of(year, future_now.getMonth(), future_now.getDayOfMonth(), 9, 29);
//                                    //system println
                                    System.out.println(start);
                                    System.out.println(end);
                                    inputToList(future_path, fs, write_list, start, end, hoze, stock_code_format);
                                }
                                else{
                                    start = now.minusMinutes(30);
                                    end = now.plusMinutes(30);
                                    inputToList(now_path, fs, write_list, start, end, hoze, stock_code_format);
                                }
                                if (write_list.size() != 61)
                                {
                                    continue;
                                }
                                Path output_file;
                                for (int i = 0; ; i++)
                                {
                                    output_file = new Path(outputFolder + '/' + stock_code_format + "_" + i);
                                    if (fs.exists(output_file) == false) {
                                        break;
                                    }
                                }
                                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(output_file)));
                                for (String l : write_list) {
                                    writer.write(l);
                                    writer.newLine();
                                }
                                writer.close();
                                System.out.println("파일 작성 완료 : " + output_file);
                            }
                        }
                    } finally {
                        IOUtils.closeStream(br);
                        IOUtils.closeStream(inputStream);
                    }
                }
            }
        }
//        outputStream.close();
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
     *  1: 첫번째 줄 값 -> False, True , stockcode =>파싱 후 stock_code를 키값으로
     *  2 : 두번째줄부터 읽은 다음, 계산 -> 앞의 줄 15줄과 중간 15줄 / 중간 15줄과 마지막 15줄을 가지고 분석
     *  2-a : 분석시 두가지 분석 -> 거래량 변화의 평균값, 가격의 변동량
     *  3 : 총 8가지 값이 나옴 -> key : stock code , value : 거래량 변화, 가격변동 * 4
     *  4 : context에 작성
     */
    public static class MyMapper
            extends Mapper<Object, Text, Text, Text> {

        /**
         * 분봉 csv파일 형식
         * prdy_vrss,prdy_vrss_sign,prdy_ctrt,stck_prdy_clpr,acml_vol,acml_tr_pbmn,hts_kor_isnm,stck_prpr,stck_bsop_date,stck_cntg_hour,stck_prpr,stck_oprc,stck_hgpr,stck_lwpr,cntg_vol,acml_tr_pbmn
         * (날짜(20230412)8, (시간 : 090000)9, (현재가)10, (1분간 거래량)14
         */
        /**
         * 실제 파일 형식 - 호재,stock_code
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            String stock_code = line[17];
            Integer date = Integer.parseInt(line[8]);
            Integer time = Integer.parseInt(line[9]);
            Integer now_price = Integer.parseInt(line[10]);
            Integer amount = Integer.parseInt(line[14]);

            String context_value = date + "," + time + "," + now_price + "," + amount;
            context.write(new Text(stock_code), new Text (context_value));

        }
    }

    /**
     * Reducer
     * Context : 호재, 악재가 key값, value의 경우 공시가 발행된 앞 뒤 60분간격의  csv파일이 한 value마다 작성되어 있음.
     * 각각의 줄을 모으고, 60 ~ 30 / 30 ~ 0 ~ 30 / 30 ~ 60까지의 데이터로 전처리함.
     * 이 줄들을 분석한 다음, 변동률, 수익률을 가지고 나이브한 형식으로 value에 작성함.
     * output : key : dart에서 분석한 호재, 악재, value : 주식 가격의 변동률, 수익률, 얼마나 작용한지
     * 1. value : now_price,amount, ... (60개)
     * 1-a. key, values 중 각각의 값을 비교해 변동값을 계산함.
     * 3. key : stock_code / value : 가격 변동률, 거래량 변동률 (30분간의 변동률(%)와 내부 30분간의 변동률(%)를 가지고 서로 비교해 백분율로 표시하게 됨.)
     */
    /**
     * 표준편차의 차이를 %로 구하자.
     */

    private static double stdDeviation(ArrayList<Integer> input)
    {
        double sumInput = input.stream().mapToInt(i -> i).average().getAsDouble();
        double variance = 0;
        for (Integer i : input)
        {
            variance += Math.pow(i - sumInput, 2);
        }
        variance = variance / input.size();
        return Math.sqrt(variance);
    }

    public static class MyReducer
            extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<Integer> before_stock_price = new ArrayList<>();
            ArrayList<Integer> after_stock_price = new ArrayList<>();
            ArrayList<Integer> before_stock_amount = new ArrayList<>();
            ArrayList<Integer> after_stock_amount = new ArrayList<>();

            Iterator<Text> iterator = values.iterator();
            for (int i = 0; i <= 60; i++)
            {
                if (!iterator.hasNext())
                {
                    break ;
                }
                Text value = iterator.next();
                String[] line = value.toString().split(",");
                if (i < 15 || i > 45)
                {
                    after_stock_price.add(Integer.parseInt(line[2]));
                    after_stock_amount.add(Integer.parseInt(line[3]));
                }
                else
                {
                    before_stock_price.add(Integer.parseInt(line[2]));
                    before_stock_amount.add(Integer.parseInt(line[3]));
                }
            }
            double stock_amount_deviation = stdDeviation(after_stock_amount) / stdDeviation(before_stock_amount);
            double stock_price_deviation = stdDeviation(after_stock_price) / stdDeviation(before_stock_price);
            context.write(key, new Text(stock_amount_deviation + "," + stock_price_deviation));
        }

        /**
         * Afterprocessing
         * 1. 구현하지 않는다.
         */
    }

    public static void main(String[] args) throws Exception {
//        String inputFolder = args[0];
//        String dartFolder = args[1];
//        String outputFolder = args[2];

        String inputFolder = args[0];
        String outputFolder = args[1];
//        preprocess(inputFolder, dartFolder, outputFolder);

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "checkStockVol");

        job.setJarByClass(checkStockVol.class);

        job.setMapperClass(checkStockVol.MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(checkStockVol.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job,new Path(inputFolder));
        FileOutputFormat.setOutputPath(job,new Path(outputFolder));

        job.waitForCompletion(true);
    }
}
