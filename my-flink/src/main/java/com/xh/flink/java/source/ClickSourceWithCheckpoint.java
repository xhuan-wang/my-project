package com.xh.flink.java.source;

import com.xh.flink.java.pojo.Event;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @author: xhuan_wang
 * @Title: ClickSourceWithCheckpoint
 * @ProjectName: my-project
 * @Description: 还有问题
 * @date: 2022-04-13 11:13
 */
public class ClickSourceWithCheckpoint implements SourceFunction<Event>, CheckpointedFunction {
    private volatile Boolean running = true;
    private Event event;
    private transient ListState<Event> eventListState;

    @Override
    public void run(SourceContext ctx) throws Exception {
        Random random = new Random();
        String[] users = {"Mary", "Alice", "Bob", "cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        while (running){
            synchronized (ctx.getCheckpointLock()){
                ctx.collect(new Event(users[random.nextInt(users.length)],
                        urls[random.nextInt(urls.length)],
                        Calendar.getInstance().getTimeInMillis()));
                Thread.sleep(1000);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        this.eventListState = context.getKeyedStateStore()
                .getListState(new ListStateDescriptor<Event>("event", Event.class));

        if (context.isRestored()){
            for (Event event: this.eventListState.get()){
                this.event = event;
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

        this.eventListState.clear();
        this.eventListState.add(event);
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new ClickSourceWithCheckpoint()).print();
        env.execute();
    }
}
