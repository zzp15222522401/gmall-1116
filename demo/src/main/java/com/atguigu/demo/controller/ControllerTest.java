package com.atguigu.demo.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controller 接受请求
 */
//@RestController = @Controller + @ResponseBody
    @RestController
public class ControllerTest {

    /**
     * 接收到请求之后处理请求
     * @return
     */
    @RequestMapping("test")
    public String abc(){
        System.out.println("123");
        return "success";
    }


    /**
     * 接收到请求之后处理请求
     * @return
     */
    @RequestMapping("test1")
    public String test02(@RequestParam("name") String na,
                         @RequestParam("age") int ag){
        System.out.println("123");
        return na+":"+ag+"岁";
    }


}
