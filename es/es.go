package es

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/olivere/elastic/v7"
)

// 将日志数据写入ES

type ESClient struct {
	client      *elastic.Client  // ES的连接
	index       string           // 连接ES的哪个数据库
	logDataChan chan interface{} // 在kafka中写入数据的通道 即接收日志的通道
	// 为了防止通道中数据 太大用空接口类型
}

// 创建上述结构体全局变量
var (
	esClient *ESClient // 为指针类型
)

func Init(maxSize, goroutineNum int, addr, index string) (err error) {
	// 注意：配置文件仅仅写的是地址，没加前面的 http://，故下方需拼接上
	client, err := elastic.NewClient(elastic.SetURL("http://" + addr))
	if err != nil {
		// Handle error
		panic(err)
	}
	// 给上述结构体指针 赋值
	esClient = &ESClient{
		client:      client,
		logDataChan: make(chan interface{}, maxSize),
		index:       index,
	}
	fmt.Println("connect to es success")
	// 后台 调用函数 从通道取出数据 在发往ES
	// 根据配置来决定起多少个goroutine在后台往ES中写数据
	for i := 0; i < goroutineNum; i++ {
		go sendToES()
	}
	return
}

// sendToES 从通道中取出数据 发送到ES
func sendToES() {
	for m1 := range esClient.logDataChan {
		// 序列化数据
		//b, err := json.Marshal(m1)
		//if err != nil {
		//	fmt.Println("marshal m1 failed, err:", err)
		//	continue
		//}
		put1, err := esClient.client.Index().
			Index(esClient.index).
			BodyJson(m1).
			Do(context.Background())
		if err != nil {
			// Handle error
			panic(err)
		}
		fmt.Printf("Indexed user %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)
	}
}

// PutlogData 通过首字母大写的函数 把包内部的通道暴露出去
// 即从包外 接收mes，发送到channel通道中
func PutlogData(msg interface{}) {
	esClient.logDataChan <- msg
}
