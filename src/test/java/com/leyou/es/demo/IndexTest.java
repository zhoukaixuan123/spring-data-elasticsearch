package com.leyou.es.demo;

import com.leyou.es.pojo.Item;
import com.leyou.es.pojo.ItemRepostory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilter;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.querydsl.QuerydslUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

/*
 *功能描述
 * @author zhoukx
 * @date 2019/5/4$
 * @description 创建索引$
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class IndexTest {

    //聚合函数的 用这个
    @Autowired
    private  ElasticsearchTemplate template;

    //这个是个接口
    //简单的 增删该查  用这个
    @Autowired
    private ItemRepostory itemRepostory;


    @Test
    public  void testCreate(){
        //创建索引库
        template.createIndex(Item.class);
        //映射关系
        template.putMapping(Item.class);

    }

    //批量添加数据
    @Test
    public void insertAdd(){


        List<Item> list = new ArrayList<>();
        list.add(new Item(1L, "11坚果手机R1", " 手机", "锤子", 3699.00, "http://image.leyou.com/123.jpg"));
        list.add(new Item(4L, "44华为META10", " 手机", "华为", 4499.00, "http://image.leyou.com/3.jpg"));

        list.add(new Item(2L, "22坚果手机R1", " 手机", "锤子", 3699.00, "http://image.leyou.com/123.jpg"));
        list.add(new Item(3L, "33华为META10", " 手机", "华为", 4499.00, "http://image.leyou.com/3.jpg"));
        // 接收对象集合，实现批量新增
        itemRepostory.saveAll(list);
    }



    //查询数据
    @Test
    public  void testFind(){
        Iterable<Item>  items =   itemRepostory.findAll();
        items.forEach(item-> System.out.println(item));

    }


    //复杂 查询过滤 可以自己写接口不用实现方法
    @Test
    public void testFindBy(){
        Iterable<Item>  items = itemRepostory.findByPriceBetween(2000d,4000d);
        items.forEach(item-> System.out.println(item));

    }


    //自定义查询
    @Test
     public void testQuery(){
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("title","11");
       Iterable<Item> items =  itemRepostory.search(queryBuilder);
        items.forEach(System.out::println);

     }


     //原声查询
     //分页查询
    //自定义分页查询
    @Test
    public void testNativeQuery(){
        // 构建查询条件
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        //结果过滤
        queryBuilder.withSourceFilter( new FetchSourceFilter(new  String []{"title","id"},null));
        // 添加基本的分词查询
        queryBuilder.withQuery(QueryBuilders.matchQuery("title", "11"));
        //排序
        queryBuilder.withSort(SortBuilders.fieldSort("price").order(SortOrder.DESC));
        //分页
        queryBuilder.withPageable(PageRequest.of(0,2));
        // 执行搜索，获取结果
        Page<Item> items = this.itemRepostory.search(queryBuilder.build());
        // 打印总条数
        System.out.println(items.getTotalElements());
        // 打印总页数
        System.out.println(items.getTotalPages());
        items.forEach(System.out::println);
    }


    //聚合条件查询
    @Test
    public void testAggs(){
        //都要用这个
        NativeSearchQueryBuilder quertBuilder = new NativeSearchQueryBuilder();
        String aggName ="popularBrand";
        //聚合条件
        quertBuilder.addAggregation(AggregationBuilders.terms(aggName).field("brand"));
        //查询返回聚合截国
     AggregatedPage<Item> items =  template.queryForPage(quertBuilder.build(),Item.class);
     //解析聚和
        //先拿到外层的对象
     Aggregations aggs =  items.getAggregations();
     //获取指定名称的聚合
      StringTerms agg =   aggs.get(aggName);
      //获取bucekt桶
      List<StringTerms.Bucket> buckets =   agg.getBuckets();
      for(StringTerms.Bucket bucket:buckets){
          // 3.4、获取桶中的key，即品牌名称
          System.out.println("key="+bucket.getKeyAsString());
          // 3.5、获取桶中的文档数量
          System.out.println("count="+bucket.getDocCount());
      }

    }
}
