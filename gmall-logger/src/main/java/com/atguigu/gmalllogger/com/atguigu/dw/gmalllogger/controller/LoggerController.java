package com.atguigu.gmalllogger.com.atguigu.dw.gmalllogger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Author: Wmd
 * Date: 2019/7/22 13:06
 */
@RestController
public class LoggerController {
    //    @RequestMapping(value = "/log", method = RequestMethod.POST)
    //    @ResponseBody  //表示返回值是一个 字符串, 而不是 页面名
    @PostMapping("/log")  // 等价于: @RequestMapping(value = "/log", method = RequestMethod.POST)
    public String doLog(@RequestParam("log") String log) {
        System.out.println(log);
        return "success";
    }

    /**
     * 业务:
     *
     * 1. 给日志添加时间戳 (客户端的时间有可能不准, 所以使用服务器端的时间)
     *
     * 2. 日志落盘
     *
     * 3. 日志发送 kafka
     */

    /**
     * 添加时间戳
     * @param log
     * @return
     */
    public String addTS(String log){
        JSONObject obj = JSON.parseObject(log);
        obj.put("ts",System.currentTimeMillis());
        return obj.toString();
    }

}

