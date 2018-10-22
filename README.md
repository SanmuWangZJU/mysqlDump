# MySQL全量数据导入工具
## 概述
- 支持导入全量数据的全部/指定列；
- 支持源端目的端列名不一致；
- 支持目标表为空/已有数据
- 支持源端为多个库表（分库分表）
- 支持目的端为分库分表

## 配置方法
### 主配置文件
- 位置：`src/main/resources/conf.yaml`
- source/target：源端和目的端的配置信息
- mediaPair: 源端和目的端的库表信息和列信息，关于列信息，具体解释如下：
    + 列名一致： 配置`columnName`
    + 列名不一致： 配置`sourceColumnName`和`targetColumnName`
- 主键/唯一索引列信息: `keyPairs`，配置方案同mediaPair的列信息配置方案

### 分库分表配置
- `conf.yaml`文件需配置:
    + 启用分库分表：`usingShard: true`
    + 标记分片键： 在`columnParis`的分片键下标记`shardKey: true`
- 分库分表走sharding-sphere，配置方案见官网描述。