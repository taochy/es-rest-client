# es-rest-client
从java transport client转化成java restful client的转换器，对restful client进行了部分封装，最大程度减少业务代码适配带来的额外工作量，部分分支做了跨es大版本的兼容
# 分支说明
main与1.0.0-restful-RELEASE_7.9.0都是基于elasticsearch7.9.0的restful client进行开发的，适用于访问7.X的elasticsearch  
1.0.0-restful-RELEASE_6.6.2都是基于elasticsearch6.6.2的restful client进行开发的，适用于访问6.X的elasticsearch  
1.0.0-restful-RELEASE_unified是基于6.6.2的restful client，并在修改了部分restful client的源码来满足跨版本兼容6.X和7.X两个大版本的elasticsearch而研发的分支用于满足特定的需求，里面涉及到修改源码的restful client，如果需要该部分源码和jar包，可以给我留言或者发邮件tao_chongyin@163.com。  
兼容版本的相关需求以及说明可以参考我的博文：https://blog.csdn.net/microGP/article/details/110938868
# 使用方法：
可以通过springboots或者java自带的方式进行es-client的初始化，然后调用方法，该工具最大的好处就是尽量兼容java transport api，减少适配的工作量，比如：  
**transport client的search代码：**  
`SearchResponse searchResponse = esClient.getClient().prepareSearch(indexType)  
        .setTypes(commonBaseService.getIndexTypeName())  
        .setQuery(boolQueryBuilder)  
        .addAggregation(field)  
        .execute().actionGet();`  
**而当前工具的search代码为：**  
`SearchResponse searchResponse = esClient.prepareSearch(indexType)  
        .setTypes(commonBaseService.getIndexTypeName())  
        .setQuery(boolQueryBuilder)  
        .addAggregation(field)  
        .execute().actionGet();`  
至于其他的request我已经在代码中留下了扩展的接口，大家可以根据自己的需求进行扩展  
另外操作集群相关的api以及未在rest-high-level-api中未实现的方法我已经通过rest-low-level-api进行了实现，并经行了封装  
之所以使用rest-low-level-api封装了部分代码是为了保证这部分可能受版本影响较大且使用频率和方式固定的api在跨版本的时候保持比较好的兼容性和稳定性
