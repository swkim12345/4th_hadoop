//package src;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;


public class DSC {

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


    public static void inputToList(Path path, long diff, FileSystem fs, ArrayList<String> write_list, boolean before)
            throws IOException
    {
        System.out.println("path : " + path.toString());
        if (fs.exists(path)) {
            BufferedReader read_csv_br = new BufferedReader(new InputStreamReader(fs.open(path)));
            String read_csv_line = read_csv_br.readLine();
            if (before)
            {
                for (int i = 0; i <= diff; i++) {
                    read_csv_line = read_csv_br.readLine();
                    if (read_csv_line == null)
                        break ;
                    if (diff - 30 < i)
                    {
                        write_list.add(read_csv_line);
                    }
                }
            }
            else
            {
                for (int i = 0; i <= diff + 30; i++) {
                    read_csv_line = read_csv_br.readLine();
                    if (read_csv_line == null)
                        break ;
                    if (diff < i)
                    {
                        write_list.add(read_csv_line);
                    }
                }
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

        //폴더를 읽어 처리하는 파일
        Path inFolder = new Path(inputFolder);
        Path daFolder = new Path(dartFolder);

//        LocalDateTime startTime = LocalDateTime.of(2023, 1,1,9,0);

//        FSDataOutputStream outputStream = fs.create(outFolder);
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
                            else
                            {
                                ArrayList<String> write_list = new ArrayList<>();
//                                write_list.add(hoze);
                                /**
                                 * 날짜, 시간, 파일 이름순
                                 */
                                /**
                                 * 분봉 csv파일 형식
                                 * prdy_vrss,prdy_vrss_sign,prdy_ctrt,stck_prdy_clpr,acml_vol,acml_tr_pbmn,hts_kor_isnm,stck_prpr,stck_bsop_date,stck_cntg_hour,stck_prpr,stck_oprc,stck_hgpr,stck_lwpr,cntg_vol,acml_tr_pbmn
                                 * (날짜(20230412)8, (시간 : 090000)9, (현재가)10, (1분간 거래량)14
                                 */
                                Integer hour = Integer.parseInt( time.split(":")[0]);
                                Integer minute = Integer.parseInt(time.split(":")[1]);
                                Integer year = Integer.parseInt(rcept_dt.substring(0, 4));
                                Integer month = Integer.parseInt(rcept_dt.substring(4, 6));
                                Integer day = Integer.parseInt(rcept_dt.substring(6, 8));
                                DateTimeFormatter folder_formatter = DateTimeFormatter.ofPattern("yyyy_MM_dd");
                                LocalDateTime now = LocalDateTime.of(year, month, day, hour, minute);
                                LocalDateTime prev_now = now;
                                LocalDateTime future_now = now;
                                /**
                                 * hour가 9시 이전 혹은 9시 반 이전일 경우 / 15시 이후 혹은 14시 50분 초과일 경우
                                 */
                                while (isWeekday(prev_now))
                                {
                                    prev_now = now.minusDays(1);
                                    System.out.println(prev_now.format(folder_formatter));
                                }
                                while (isWeekday(future_now))
                                {
                                    future_now = future_now.plusDays(1);
                                    System.out.println(future_now.format(folder_formatter));
                                }

                                //TODO : error in this code
                                if (hour < 9 || (hour == 9 && minute < 30) || hour >= 15 || (hour == 14 && minute < 50))
                                {
                                    prev_now = prev_now.withHour(15).withMinute(20).withSecond(0).withNano(0);
                                    future_now = future_now.withHour(9).withMinute(0).withSecond(0).withNano(0);
                                    if (hour < 9 || (hour == 9 && minute < 30))
                                    {
                                        prev_now = prev_now.minusDays(1);
                                        while (isWeekday(prev_now))
                                        {
                                            prev_now = prev_now.minusDays(1);
                                        }
                                    }
                                    else {
                                        future_now = future_now.plusDays(1);
                                        while (isWeekday(future_now)) {
                                            future_now = future_now.plusDays(1);
                                        }
                                    }
                                }
                                String prev_str = getDir(new String[]{inputFolder, prev_now.format(folder_formatter)});
                                String future_str = getDir(new String[]{inputFolder, future_now.format(folder_formatter)});
                                String kospi = "kospi";
                                String kosdaq = "kosdaq";

                                System.out.println("prev_str" + prev_str);
                                System.out.println("future_str" + future_str);
                                String stock_code_format = String.format("%06d", Integer.parseInt(stock_code));

                                write_list.add(hoze + ',' + stock_code_format);
                                System.out.println("format : " + stock_code_format);
                                //실제 파일이 존재하는 지 확인하는 코드
                                Path prev_kospi_path = new Path(getDir(new String[]{prev_str, kosdaq, stock_code_format}) + ".csv");
                                Path prev_kosdaq_path = new Path(getDir(new String[]{prev_str, kospi, stock_code_format}) + ".csv");
                                Path future_kospi_path = new Path(getDir(new String[]{future_str, kosdaq, stock_code_format}) + ".csv");
                                Path future_kosdaq_path = new Path(getDir(new String[]{future_str, kospi, stock_code_format}) + ".csv");

                                long diff = (prev_now.getHour() - 9 ) * 60 + prev_now.getMinute();
                                System.out.println("diff : " + diff);
                                inputToList(prev_kospi_path, diff, fs, write_list, true);
                                inputToList(prev_kosdaq_path, diff, fs, write_list, true);
                                diff = (future_now.getHour() - 9 ) * 60 + future_now.getMinute();
                                System.out.println("diff : " + diff);
                                inputToList(future_kospi_path, diff, fs, write_list, false);
                                inputToList(future_kosdaq_path, diff, fs, write_list, false);

                                if (write_list.size() != 60)
                                {
                                    continue ;
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
            private Text word = new Text();

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
                String[] line = value.toString().split("\n");
                String hoze = line[0].split(",")[0];
                String stock_code = line[0].split(",")[1];
                ArrayList<Integer> now_price = new ArrayList<>();
                ArrayList<Integer> amount = new ArrayList<>();
                for (String s : line) {
                    String[] stock_minute_csv = s.split(",");
                    if (hoze.equals(stock_minute_csv[0])) {
                        continue;
                    }
                    now_price.add(Integer.parseInt(stock_minute_csv[10]));
                    amount.add(Integer.parseInt(stock_minute_csv[14]));

                }
                String context_value = new String();
                for (int i = 0; i < 60; i++)
                {
                    context_value += now_price.get(i) + "," + amount.get(i) + ",";
                }
                context.write(new Text(stock_code), new Text (context_value));
//                for (int j = 0; j < 4; j++)
//                {
//                    Integer now_price_
//                    for (int i = 0; i < 15; i++)
//                    {
//
//                    }
//                }
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
        public static class MyReducer
                extends Reducer<Text, Text, Text, Text> {
            protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                Integer before_amount;
                Integer after_amount;
                Integer before_stock_price;
                Integer after_stock_price;
                for (Text value : values) {

                }
            }

            /**
             * Afterprocessing
             * 1. 구현하지 않는다.
             */
        }

    public static void main(String[] args) throws Exception {
        String inputFolder = args[0];
        String dartFolder = args[1];
        String outputFolder = args[2];

        Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "preprocess");
//
//            job.setJarByClass(preprocess.class);

            //preprocessing finished
//        preprocess(inputFolder, dartFolder, outputFolder);

//        java.lang.module.Configuration conf = new java.lang.module.Configuration();

        job.setJarByClass(DSC.class);

        job.setMapperClass(DSC.MyMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(DSC.MyReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job,new Path(inputFolder));
        FileOutputFormat.setOutputPath(job,new Path(outputFolder));

        job.waitForCompletion(true);

    }
}