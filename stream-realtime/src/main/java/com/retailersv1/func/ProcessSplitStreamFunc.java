package com.retailersv1.func;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ProcessSplitStreamFunc extends ProcessFunction<JSONObject, String>{
    private OutputTag<String> errTag;
    private OutputTag<String> startTag;
    private OutputTag<String> displayTag;
    private OutputTag<String> actionTag;

    public ProcessSplitStreamFunc(OutputTag<String> errTag, OutputTag<String> startTag, OutputTag<String> displayTag, OutputTag<String> actionTag) {
        this.errTag = errTag;
        this.startTag = startTag;
        this.displayTag = displayTag;
        this.actionTag = actionTag;
    }

    @Override
    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
        JSONObject errJson = jsonObject.getJSONObject("err");
        if (errJson != null){
            context.output(errTag,errJson.toJSONString());
            jsonObject.remove("err");
        }
        JSONObject startJsonObj = jsonObject.getJSONObject("start");
        if (startJsonObj != null){
            context.output(startTag,jsonObject.toJSONString());
        }else {
            JSONObject commonJsonObj = jsonObject.getJSONObject("common");
            JSONObject pageJsonObj = jsonObject.getJSONObject("page");
            Long ts = jsonObject.getLong("ts");
            JSONArray displayArr = jsonObject.getJSONArray("displays");
            if (displayArr != null && displayArr.size() > 0){
                for (int i = 0; i < displayArr.size(); i++) {
                    JSONObject displayJsonObj = displayArr.getJSONObject(i);
                    JSONObject newDisplayJsonObj = new JSONObject();
                    newDisplayJsonObj.put("common",commonJsonObj);
                    newDisplayJsonObj.put("page",pageJsonObj);
                    newDisplayJsonObj.put("display",displayJsonObj);
                    newDisplayJsonObj.put("ts",ts);

                    context.output(displayTag,newDisplayJsonObj.toJSONString());
                }
                jsonObject.remove("displays");
            }
            JSONArray actionArr = jsonObject.getJSONArray("actions");
            if (actionArr != null && actionArr.size() > 0){
                for (int i = 0; i < actionArr.size(); i++) {
                    JSONObject actionJsonObj = actionArr.getJSONObject(i);
                    JSONObject newActionJsonObj = new JSONObject();
                    newActionJsonObj.put("common",commonJsonObj);
                    newActionJsonObj.put("page",pageJsonObj);
                    newActionJsonObj.put("action",actionJsonObj);

                    context.output(actionTag,newActionJsonObj.toJSONString());
                }
                jsonObject.remove("actions");
            }
            collector.collect(jsonObject.toJSONString());
        }
    }
}
