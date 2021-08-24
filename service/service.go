package service

import (
	"github.com/olivere/elastic/v7"
	"github.com/tianxinbaiyun/mysql2es/config"
	"github.com/tianxinbaiyun/mysql2es/database"
	"log"
)

// Sync 同步函数
func Sync() {
	// 变量定义
	var (
		err      error
		rows     []*elastic.SearchHit
		offset   int64
		fistFlag bool
	)

	// 读取配置文件到struct,初始化变量
	config.InitConfig()

	// 连接数据库
	database.InitSrcES()
	database.InitTargetES()

	// 如果配置重建，则清空数据
	if config.C.Others.Rebuild {
		// 如果不存在，创建索引
		if !database.GetTargetESIndexExist(config.C.TargetEs.Index) {
			err = database.CreateTargetESIndex(config.C.TargetEs.Index)
			if err != nil {
				return
			}
		}
	}

	fistFlag = true
	syncCount := 0

	for fistFlag || len(rows) > 0 {

		// 从新获取数据
		rows, offset, err = database.GetSrcESList(config.C.SrcEs.Index, offset, config.C.Others.Batch)
		if err != nil {
			log.Println("err:", err)
			return
		}

		rowLen := len(rows)

		if rowLen <= 0 {
			break
		}
		fistFlag = false

		// 循环插入数据
		for _, row := range rows {
			err = database.UpdateTargetES(config.C.TargetEs.Index, row.Id, row.Source)
			if err != nil {
				log.Println("err:", err)
				return
			}
		}

		// 统计同步数量
		syncCount = syncCount + rowLen

		// 如果返回数量小于size，结束循环
		if int64(rowLen) < config.C.Others.Batch {
			break
		}
	}
	log.Printf("sync done index name:%s  sync count %d", config.C.TargetEs.Index, syncCount)

	return
}
