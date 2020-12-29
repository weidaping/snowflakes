import java.util.Random;


/**
 * 基于雪花算法的分布式递增id生成器
 * <p>
 * 组成：
 * （1）第一个部分，是 1 个 bit：0，这个是无意义的。
 * （2）第二个部分是 41 个 bit：表示的是时间基数，初始化时用当前时间戳。
 * （3）第三个部分是 12 个 bit：表示的序号，从0开始递增，到达最大值后，时间基数 + 1。
 * （4）第四个部分是 6 个 bit：表示的是机器 id，11 1001。
 * （5）第五个部分是 4 个 bit：随机数random值
 * <p>
 * 采用初始化时的时间戳为时间基数baseTimestamp，
 * 时间基数baseTimestamp关联序列号sequence递增，
 * 最后4bit用random，
 * 用时间基数 + random这种方式解决时间回拨问题
 * <p>
 * 并且由于采用时间基数代替每次获取当前时间戳的方式，一段时间后会有存量id，每秒可产生的id数更多
 *
 * @author weidaping
 * @since 2020-12-29
 */
public class IdWorker {
    //因为二进制里第一个 bit 为如果是 1，那么都是负数，但是我们生成的 id 都是正数，所以第一个 bit 统一都是 0。

    //机器ID  2进制6位  64位减掉1位 63个
    private long workerId;
    //随机数字 2进制4位  16位减掉1位 15个
    private long randomNumber;
    //代表一毫秒内生成的多个id的最新序号  12位 4096 -1 = 4095 个
    private long sequence;
    //设置一个时间初始值    2^41 - 1   差不多可以用69年
    private long twepoch = 1585644268888L;
    //6位的机器id
    private long workerIdBits = 6L;
    //4位的random数字
    private long randomNumberBits = 4L;
    //每毫秒内产生的id数 2 的 12次方
    private long sequenceBits = 12L;
    // 这个是二进制运算，就是6 bit最多只能有63个数字，也就是说机器id最多只能是64以内
    private long maxWorkerId = -1L ^ (-1L << workerIdBits);
    // 这个是一个意思，就是4 bit最多只能有15个数字，随机数字最多只能是16以内
    private int randomNumberBound = 1 << randomNumberBits;

    private long workerIdShift = randomNumberBits;
    private long sequenceShift = randomNumberBits + workerIdBits;
    private long timestampLeftShift = sequenceBits + workerIdBits + randomNumberBits;
    private long sequenceMask = -1L ^ (-1L << sequenceBits);

    //记录初始的时间毫秒数
    private long baseTimestamp = -1L;


    public long getWorkerId() {
        return workerId;
    }

    public long getTimestamp() {
        return System.currentTimeMillis();
    }


    public IdWorker(long workerId, long sequence) {

        // 检查机房id和机器id是否超过31 不能小于0
        if (workerId > maxWorkerId || workerId < 0) {
            throw new IllegalArgumentException(
                    String.format("worker Id can't be greater than %d or less than 0", maxWorkerId));
        }

        this.workerId = workerId;
        this.sequence = sequence;
        this.baseTimestamp = System.currentTimeMillis();
    }

    // 这个是核心方法，通过调用nextId()方法，让当前这台机器上的snowflake算法程序生成一个全局唯一的id
    public synchronized long nextId() {
        //当前时间戳
        long timestamp = this.timeGen();

        //如果时间基数超过了当前时间，则沉睡直到当前时间超过时间基数，再生成id
        if (timestamp < baseTimestamp) {

            System.err.printf(
                    "clock is moving backwards. Rejecting requests until %d.", baseTimestamp);
            this.tilNextMillis(baseTimestamp);
        }

        // 这个意思是说一个毫秒内最多只能有4096个数字，无论你传递多少进来，
        //这个位运算保证始终就是在4096这个范围内，避免你自己传递个sequence超过了4096这个范围
        sequence = (sequence + 1) & sequenceMask;
        //当某一毫秒的时间，产生的id数 超过4095，时间基数加一毫秒，系统继续产生ID
        if (sequence == 0) {
            baseTimestamp++;
        }

        //随机数生成
        randomNumber = new Random().nextInt(randomNumberBound);

        // 这儿就是最核心的二进制位运算操作，生成一个64bit的id
        // 先将当前时间戳左移，放到41 bit那儿；将序号左移放到12 bit那儿；将机器id左移放到6 bit那儿；将随机数放最后4 bit
        // 最后拼接起来成一个64 bit的二进制数字，转换成10进制就是个long型
        return ((baseTimestamp - twepoch) << timestampLeftShift) |
                (sequence << sequenceShift) |
                (workerId << workerIdShift) | randomNumber;
    }


    /**
     * 当时间基数超过当前时间戳，系统会进入等待，直到当前时间追回时间基数，系统继续产生ID
     *
     * @param baseTimestamp
     * @return
     */
    private long tilNextMillis(long baseTimestamp) {

        long timestamp = timeGen();

        while (timestamp <= baseTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }


    /**
     * 获取当前时间戳
     */
    private long timeGen() {
        return System.currentTimeMillis();
    }


    /**
     * main 测试类
     *
     * @param args
     */
    public static void main(String[] args) {
        IdWorker worker = new IdWorker(5, 1);

//        System.out.println(1 << worker.randomNumberBits);
        long start = System.currentTimeMillis();
        for (int i = 0; i < 620000; i++) {
            System.out.println(worker.nextId());
        }

        long timeUse = System.currentTimeMillis() - start;
        System.out.println("time use：" + timeUse + "ms");
    }


}
