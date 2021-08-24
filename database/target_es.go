package database

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	"github.com/tianxinbaiyun/es2es/config"
	"log"
)

// TargetESClient es客户端
var (
	TargetESClient *elastic.Client
)

// InitTargetES InitTargetES
func InitTargetES() {
	GetTargetESClient(config.C.TargetEs)
}

// GetTargetESClient 获取客户端，获取GetTargetESClient
func GetTargetESClient(conn config.EsConn) *elastic.Client {
	if TargetESClient != nil {
		return TargetESClient
	}
	client, err := elastic.NewClient(elastic.SetURL(fmt.Sprintf("http://%s:%s", conn.Host, conn.Port)), elastic.SetSniff(false))
	if err != nil {
		panic(err)
	}
	TargetESClient = client
	return TargetESClient
}

// CreateTargetESIndex 创建索引
func CreateTargetESIndex(index string) (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err = TargetESClient.CreateIndex(index).Do(ctx)
	if err != nil {
		log.Println(err)
		return
	}
	return
}

// GetTargetESIndexExist 索引是否存在
func GetTargetESIndexExist(index string) (ok bool) {
	ok, err := TargetESClient.IndexExists().Index([]string{index}).Do(context.Background())
	if err != nil {
		log.Println(err)
		return
	}
	return
}

// Refresh 刷新数据
func Refresh(index string) (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err = TargetESClient.Refresh(index).Do(ctx)
	//time.Sleep(time.Second)
	return
}

// UpdateTargetES UpdateTargetES 更新
func UpdateTargetES(index, id string, data interface{}) (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res, err := TargetESClient.Index().
		Index(index).
		Id(id).
		BodyJson(data).
		Do(ctx)
	if err != nil {
		log.Println(err.Error())
	}
	fmt.Println(res)
	return
}
