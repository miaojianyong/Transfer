package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/log_transfer/es"
)

// 初始化kafka连接
// 从kafka里面取出日志数据

func Init(adder []string, topic string) (err error) {
	// 创建新的消费者
	consumer, err := sarama.NewConsumer(adder, nil)
	if err != nil {
		fmt.Printf("fail to start consumer, err:%v", err)
		return
	}
	// 拿到指定的 topic 下面所有分区的列表
	partitionList, err := consumer.Partitions(topic)
	if err != nil {
		fmt.Printf("fail to get partition, err:%v", err)
		return
	}
	// 遍历所有的分区
	for partition := range partitionList {
		// 针对每个分区 创建一个对应的分区消费者
		pc, err := consumer.ConsumePartition(topic,
			int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("fail to start consumer for partition %d, err:%v",
				partition, err)
			return
		}
		//defer pc.AsyncClose() // 这里关闭后 下方后台的goroutine就读取不到pc
		// 异步从每个分区 去读消费者信息
		go func(sarama.PartitionConsumer) {
			// 好像可以在这 里面关闭
			defer pc.AsyncClose()
			for msg := range pc.Messages() {
				// 为了将同步流程异步化 把数据先存放到通道中
				// 然后ES那边可以从通道中读取数据 再存放到ES
				var m1 map[string]interface{}
				err = json.Unmarshal(msg.Value, &m1)
				if err != nil {
					fmt.Println("unmarshal mas failed, err:", err)
					continue
				}
				// 反正数据太大，用map,即它是引用类型
				es.PutlogData(m1)
			}
		}(pc)
	}
	return
}
