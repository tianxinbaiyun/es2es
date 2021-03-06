package database

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	"github.com/tianxinbaiyun/es2es/config"
	"log"
	"os"
)

// SrcESClient es客户端
var (
	SrcESClient *elastic.Client
)

// InitSrcES InitSrcES
func InitSrcES() {
	GetSrcESClient(config.C.SrcEs)
}

// GetSrcESClient 获取客户端，获取GetSrcESClient
func GetSrcESClient(conn config.EsConn) *elastic.Client {
	if SrcESClient != nil {
		return SrcESClient
	}
	options := make([]elastic.ClientOptionFunc, 0)
	// 连接地址
	if conn.Host != "" {
		options = append(options, elastic.SetURL(fmt.Sprintf("http://%s:%s", conn.Host, conn.Port)))
	}
	// 开启日志追踪
	if conn.Trace {
		options = append(options, elastic.SetTraceLog(log.New(os.Stdout, "", 0)))
	}
	// 是否开启嗅针
	options = append(options, elastic.SetSniff(false))

	// 密码设置
	if conn.User != "" {
		options = append(options, elastic.SetBasicAuth(conn.User, conn.Password))
	}
	client, err := elastic.NewClient(options...)
	if err != nil {
		panic(err)
	}
	SrcESClient = client
	return SrcESClient
}

// GetAIFaqList 查询问答列表
func GetSrcESList(index string, limit, offset int64) (list []*elastic.SearchHit, count int64, err error) {
	list = make([]*elastic.SearchHit, 0)

	search := SrcESClient.Search(index)
	if limit > 0 {
		search = search.Size(int(limit))
	} else {
		search = search.Size(999)
	}
	if offset > 0 {
		search = search.From(int(offset))
	}

	search = search.IgnoreUnavailable(true)
	// 查询数据
	ctx := context.Background()
	resp, err := search.Do(ctx)
	if err != nil {
		return
	}
	if resp.Hits == nil || len(resp.Hits.Hits) == 0 {
		return
	}

	count = resp.TotalHits()
	list = resp.Hits.Hits
	//for _, hit := range resp.Hits.Hits {
	//	item := &po.AIFaq{}
	//	err = json.Unmarshal(hit.Source, item)
	//	if err != nil {
	//		return
	//	}
	//	item.Id = hit.Id
	//	list = append(list, item)
	//}
	return
}
