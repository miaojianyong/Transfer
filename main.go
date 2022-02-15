package main

import (
	"fmt"
	"github.com/go-ini/ini"
	"github.com/log_transfer/es"
	"github.com/log_transfer/kafka"
	"github.com/log_transfer/model"
)

// log_transfer
// 从kafka消费者日志数据，写入ES

func main() {
	var cfg = new(model.Config) // 得到结构体对应指针
	// 1. 加载配置文件
	// 使用go-ini插件 读取配置文件
	err := ini.MapTo(cfg, "./config/logtransfer.ini")
	if err != nil {
		fmt.Println("load config failed, err:", err)
		panic(err)
	}
	fmt.Println("load config success!!!")

	// 下述2,3步骤可能需要换一下
	// 2. 连接kafka
	err = kafka.Init([]string{cfg.KafkaConf.Address}, cfg.KafkaConf.Topic)
	if err != nil {
		fmt.Println("Init kafka failed, err:", err)
		panic(err)
	}
	fmt.Println("Init kafka success!!!")
	// 3. 连接ES
	err = es.Init(cfg.ESConf.MaxSize, cfg.ESConf.GoNum, cfg.ESConf.Address, cfg.ESConf.Index)
	if err != nil {
		fmt.Println("Init ES failed, err:", err)
		panic(err)
	}
	fmt.Println("Init ES success!!!")
	// 让主函数一直处于运行 注：select不占CPU
	select {}
}
